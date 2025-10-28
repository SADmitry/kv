package sdmitry.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Append-only writer for a single segment file (%020d.seg).
 *
 * Does:
 *   - open/create a segment and append records;
 *   - return per-record Position;
 *   - expose current size (useful for rotation);
 *   - allow explicit fsync().
 *
 * Concurrency:
 *   - methods that write are synchronized → one writer per segment, order is preserved;
 *   - size is tracked with an AtomicLong to compute starting offsets;
 *   - readers should open their own read-only FileChannel — this class is write-only.
 *
 * Durability:
 *   - fsync() calls FileChannel.force(true) (data+metadata); usually done on a schedule (group commit);
 *   - records are framed+CRC’d; after a crash recovery scans until a torn tail and stops cleanly.
 *
 * Why a separate class:
 *   - keeps low-level I/O and naming away from engine logic;
 *   - centralizes offset math/order guarantees (avoids batch-offset bugs).
 */
public final class SegmentWriter implements AutoCloseable {
    private final long id;
    private final Path path;
    private final FileChannel channel;
    private final AtomicLong size;
    private final long maxBytes;

    /**
     * Open (or create) a segment for appending.
     *
     * @param dir       target directory
     * @param id        monotonically increasing segment id (used in filename {@code %020d.seg})
     * @param maxBytes  soft size limit used by rotation policies (not enforced here)
     */
    public SegmentWriter(Path dir, long id, long maxBytes) throws IOException {
        this.id = id;
        this.maxBytes = maxBytes;
        this.path = dir.resolve(String.format("%020d.seg", id));
        this.channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.size = new AtomicLong(channel.size());
    }

    /** Unique id of the segment (monotonic). */
    public long id() { return id; }

    /** Current file size in bytes (may increase after each append). */
    public long size() { return size.get(); }

    public long maxBytes() { return maxBytes; }

    /**
     * Append a single record and return its {@link Position}.
     * Method is synchronized to serialize writes and preserve order.
     */
    public synchronized Position append(Record r) throws IOException {
        long off = size.get();
        channel.write(r.toByteBuffer(), off);
        size.addAndGet(r.bytes());
        return new Position(id, off);
    }

    /**
     * Append a batch of records atomically with respect to interleaving (no other thread can interleave).
     * Returns a {@code List<Position>} with per-record starting offsets in the same order as input.
     *
     * <p>Why not gather writes? We intentionally avoid building one giant buffer to keep peak memory low
     * and to preserve per-record framing; OS/page cache will still coalesce writes effectively.</p>
     */
    public synchronized List<Position> appendMany(List<Record> records) throws IOException {
        long cur = size.get();
        List<Position> out = new ArrayList<>(records.size());
        for (Record r : records) {
            out.add(new Position(id, cur));
            ByteBuffer buf = r.toByteBuffer();
            channel.write(buf, cur);
            cur += r.bytes();
        }
        size.set(cur);
        return out;
    }

    /**
     * Force all writes to stable storage (data + metadata).
     * Typically invoked periodically by the engine (group commit).
     */
    public void fsync() throws IOException {
        channel.force(true);
    }

    /** Close the underlying channel. The segment remains readable by other readers. */
    @Override
    public void close() throws IOException {
        channel.close();
    }

    /** Absolute file path of the segment (useful for debugging/metrics). */
    public Path path() { return path; }
}
