package sdmitry.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Storage engine for a Bitcask-style, append-only KV.
 *
 * Does:
 *   - keep an in-memory index: key -> Position;
 *   - append records to segment files and rotate by size;
 *   - rebuild the index on startup by scanning segments (CRC-guarded);
 *   - serve range reads via ConcurrentSkipListMap.subMap();
 *   - compact on demand (rewrite live set into a fresh segment).
 *
 * Concurrency:
 *   - index is concurrent; writes go through a single SegmentWriter (synchronized);
 *   - rotation swaps the active writer under a lock;
 *   - readers open their own read-only channels (no sharing with the writer).
 *
 * Durability:
 *   - periodic fsync() (group commit) balances latency and throughput;
 *   - CRC at record header lets recovery stop cleanly at a torn tail.
 *
 * Notes:
 *   - append-only keeps write path sequential and simple; index stores only Positions;
 *   - format details live in Record; files are named %020d.seg.
 */
public final class BitcaskStorageEngine implements AutoCloseable, KeyValueStore {

    private final Path dir;
    private final long maxSegmentBytes;
    private final long fsyncIntervalMs;

    private final ConcurrentSkipListMap<String, Position> index = new ConcurrentSkipListMap<>();
    private volatile SegmentWriter active;
    private final Object rotateLock = new Object();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> Thread.ofVirtual().name("fsync").factory().newThread(r));

    private long nextSegmentId = 0L;

    /**
     * @param dir              data directory; will be created if absent
     * @param maxSegmentBytes  rotation threshold (soft limit) per segment
     * @param fsyncIntervalMs  group-commit fsync period; set to 0 to disable scheduled fsync
     */
    public BitcaskStorageEngine(Path dir, long maxSegmentBytes, long fsyncIntervalMs) {
        this.dir = Objects.requireNonNull(dir, "dir");
        this.maxSegmentBytes = maxSegmentBytes;
        this.fsyncIntervalMs = fsyncIntervalMs;
    }

    /** Initialize the engine: discover segments, rebuild index, open an active writer, start fsync scheduler. */
    @Override
    public void start() throws IOException {
        Files.createDirectories(dir);

        // Discover existing segments
        List<Path> segs = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.seg")) {
            for (Path p : ds) segs.add(p);
        }
        segs.sort(Comparator.naturalOrder());

        long maxId = -1;
        for (Path p : segs) {
            String fname = p.getFileName().toString();
            int dot = fname.indexOf('.');
            if (dot <= 0) continue;
            long id = Long.parseLong(fname.substring(0, dot));
            maxId = Math.max(maxId, id);
            rebuildIndexFrom(p, id);
        }
        nextSegmentId = maxId + 1;

        rotate(); // open active

        if (fsyncIntervalMs > 0) {
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    SegmentWriter w = active;
                    if (w != null) w.fsync();
                } catch (Throwable ignore) { /* best-effort */ }
            }, fsyncIntervalMs, fsyncIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * PUT a single key/value.
     * <p>Updates the in-memory index after a successful append.</p>
     */
    @Override
    public void put(String key, byte[] value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Record r = Record.normal(key.getBytes(StandardCharsets.UTF_8), value);
        Position p = active.append(r);
        index.put(key, p);
        maybeRotate();
    }

    /**
     * Batch PUT multiple key/value pairs.
     * <p>Appends records contiguously and installs per-record positions in the index.</p>
     *
     * @return number of written entries
     */
    @Override
    public int batchPut(List<Map.Entry<String, byte[]>> items) throws IOException {
        if (items == null || items.isEmpty()) return 0;
        List<Record> rs = new ArrayList<>(items.size());
        for (var e : items) {
            rs.add(Record.normal(e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue()));
        }
        List<Position> poses = active.appendMany(rs);
        for (int i = 0; i < items.size(); i++) {
            index.put(items.get(i).getKey(), poses.get(i));
        }
        maybeRotate();
        return items.size();
    }

    /** GET the latest value for a key, or {@code null} if absent or tombstoned. */
    @Override
    public byte[] read(String key) throws IOException {
        Position pos = index.get(key);
        return (pos == null) ? null : readAt(pos);
    }

    /**
     * Lexicographic range scan (inclusive).
     * <p>Returns up to {@code limit} live entries ordered by key.</p>
     */
    @Override
    public List<Map.Entry<String, byte[]>> readKeyRange(String startInclusive, String endInclusive, int limit) throws IOException {
        List<Map.Entry<String, byte[]>> out = new ArrayList<>();
        for (var e : index.subMap(startInclusive, true, endInclusive, true).entrySet()) {
            if (out.size() >= limit) break;
            byte[] v = readAt(e.getValue());
            if (v != null) {
                out.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), v));
            }
        }
        return out;
    }

    /** DELETE a key via a tombstone and evict it from the in-memory index. */
    @Override
    public void delete(String key) throws IOException {
        Objects.requireNonNull(key, "key");
        Record r = Record.tombstone(key.getBytes(StandardCharsets.UTF_8));
        active.append(r);
        index.remove(key);
        maybeRotate();
    }

    /**
     * Compact live set into a fresh segment and delete obsolete segment files.
     * Algorithm:
     *  - Snapshot the current index entries (key, Position).</li>
     *  - Write each live entry as a fresh record into a new segment.</li>
     *  - Swap the active writer to the new segment.</li>
     *  - Delete all older segments (best-effort), return reclaimed byte count.</li>
     */
    @Override
    public long compact() throws IOException {
        // Snapshot
        List<Map.Entry<String, Position>> snapshot = new ArrayList<>(index.entrySet());

        // Readying new seg
        SegmentWriter newSeg = new SegmentWriter(dir, nextSegmentId++, maxSegmentBytes);
        ConcurrentSkipListMap<String, Position> newIndex = new ConcurrentSkipListMap<>();

        // Rewrite
        for (var e : snapshot) {
            byte[] val = readAt(e.getValue());
            if (val == null) continue;
            Position p = newSeg.append(Record.normal(e.getKey().getBytes(StandardCharsets.UTF_8), val));
            newIndex.put(e.getKey(), p);
        }
        newSeg.fsync();

        // Atomically publish
        List<Path> toDelete = new ArrayList<>();
        synchronized (rotateLock) {
            SegmentWriter old = active;
            active = newSeg;

            // Save all seg's
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.seg")) {
                String keepPrefix = String.format("%020d", active.id());
                for (Path p : ds) {
                    if (!p.getFileName().toString().startsWith(keepPrefix)) {
                        toDelete.add(p);
                    }
                }
            }

            // Reset index
            index.clear();
            index.putAll(newIndex);

            if (old != null) old.close();
        }

        // Delete old segments
        long reclaimed = 0;
        for (Path p : toDelete) {
            try {
                reclaimed += Files.size(p);
                Files.deleteIfExists(p);
            } catch (IOException ignore) { /* best-effort */ }
        }
        return reclaimed;
    }


    /** Close active resources and stop background tasks. */
    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        SegmentWriter w = active;
        if (w != null) w.close();
    }

    // --------- internals ---------

    private void maybeRotate() throws IOException {
        SegmentWriter w = active;
        if (w != null && w.size() >= maxSegmentBytes) {
            rotate();
        }
    }

    /** Create a new active segment writer, closing the previous one. */
    private void rotate() throws IOException {
        synchronized (rotateLock) {
            SegmentWriter next = new SegmentWriter(dir, nextSegmentId++, maxSegmentBytes);
            SegmentWriter prev = active;
            active = next;
            if (prev != null) prev.close();
        }
    }

    /**
     * Read the record value at a given position.
     * <p>Opens a fresh read-only channel; this keeps the writer simple and avoids cross-thread sharing.</p>
     */
    private byte[] readAt(Position pos) throws IOException {
        Path seg = dir.resolve(String.format("%020d.seg", pos.segmentId()));
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(4 + 1 + 4 + 4);
            int n = ch.read(header, pos.offset());
            if (n < header.capacity()) return null; // truncated
            header.flip();
            header.getInt();               // crc (ignored on hot path)
            byte tomb = header.get();
            int klen = header.getInt();
            int vlen = header.getInt();
            if (tomb == 1) return null;    // tombstone
            ByteBuffer val = ByteBuffer.allocate(vlen);
            ch.read(val, pos.offset() + 4 + 1 + 4 + 4 + klen);
            return val.array();
        }
    }

    /**
     * Recovery: sequentially scan a segment, validate CRC, and update the in-memory index.
     * <p>If a torn/corrupted tail is detected, scanning stops gracefully.</p>
     */
    private void rebuildIndexFrom(Path seg, long id) throws IOException {
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
            long pos = 0;
            ByteBuffer header = ByteBuffer.allocate(4 + 1 + 4 + 4);
            while (pos < ch.size()) {
                header.clear();
                int n = ch.read(header, pos);
                if (n < header.capacity()) break; // partial tail
                header.flip();
                int crcStored = header.getInt();
                byte tomb = header.get();
                int klen = header.getInt();
                int vlen = header.getInt();

                long payloadPos = pos + header.capacity();
                ByteBuffer kbuf = ByteBuffer.allocate(klen);
                ByteBuffer vbuf = ByteBuffer.allocate(vlen);
                int nk = ch.read(kbuf, payloadPos);
                int nv = ch.read(vbuf, payloadPos + klen);
                if (nk < klen || nv < vlen) break; // partial tail

                kbuf.flip(); vbuf.flip();
                byte[] kbytes = new byte[klen]; kbuf.get(kbytes);
                byte[] vbytes = new byte[vlen]; vbuf.get(vbytes);

                int crcCalc = Record.computeCrc32(tomb, kbytes, vbytes);
                if (crcCalc != crcStored) {
                    // corruption: stop before this record
                    break;
                }

                String key = new String(kbytes, StandardCharsets.UTF_8);
                if (tomb == 1) {
                    index.remove(key);
                } else {
                    index.put(key, new Position(id, pos));
                }

                pos = payloadPos + klen + vlen;
            }
        }
    }
}
