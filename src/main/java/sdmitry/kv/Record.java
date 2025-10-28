package sdmitry.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * One log record in our append-only store.
 *
 * Wire format (big-endian):
 *   [crc32(4)][tomb(1)][klen(4)][vlen(4)][key][value]
 * where:
 *   - crc32  = CRC of (tomb, key, value) via java.util.zip.CRC32
 *   - tomb   = 0 (PUT) or 1 (DELETE/tombstone)
 *   - klen   = key length in bytes
 *   - vlen   = value length in bytes (0 for tombstones)
 *   - key    = UTF-8 bytes (engine uses UTF-8; caller can provide bytes)
 *   - value  = raw bytes
 *
 * Notes:
 *   - Append-only keeps writes sequential and recovery simple.
 *   - CRC is first so recovery can stop cleanly on a torn tail.
 *   - Big-endian via ByteBuffer’s default; common for on-disk formats.
 *   - A tiny immutable object with a single toByteBuffer() keeps writers simple.
 *
 * Why a separate class:
 *   - Keeps the on-disk format isolated from engine logic.
 *   - Easier to evolve (flags, checksum choice, versioning) without touching the engine.
 *   - Avoids format drift when multiple places serialize records.
 *
 * Naming:
 *   - Yes, Java has `java.lang.Record`, but this is a regular `final class`.
 *     We keep the name `Record` to match Bitcask literature; rename to `KvEntry` if you prefer.
 */
public class Record {
    /**
     * 0 = PUT, 1 = DELETE (tombstone).
     */
    private final byte tomb;
    /**
     * Key bytes (caller decides encoding; engine uses UTF-8 for String keys).
     */
    private final byte[] key;
    /**
     * Value bytes; empty array for tombstone.
     */
    private final byte[] value;
    /**
     * CRC32 over (tomb, key, value).
     */
    private final int crc;
    /**
     * Total serialized length in bytes.
     */
    private final int bytes;

    private Record(byte tomb, byte[] key, byte[] value) {
        this.tomb = tomb;
        this.key = Objects.requireNonNull(key, "key");
        this.value = Objects.requireNonNull(value, "value");
        this.crc = computeCrc32(tomb, key, value);
        this.bytes = 4 + 1 + 4 + 4 + key.length + value.length;
    }

    /**
     * Creates a normal PUT record.
     */
    public static Record normal(byte[] key, byte[] value) {
        return new Record((byte) 0, key, value);
    }

    /**
     * Creates a DELETE tombstone record (value length is zero).
     */
    public static Record tombstone(byte[] key) {
        return new Record((byte) 1, key, new byte[0]);
    }

    /**
     * Serialize to a fresh ByteBuffer positioned at 0 and flipped for reading.
     * The buffer capacity equals {@link #bytes()}.
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(bytes);
        buf.putInt(crc)
                .put(tomb)
                .putInt(key.length)
                .putInt(value.length)
                .put(key)
                .put(value)
                .flip();
        return buf;
    }

    /**
     * Re-compute CRC32 (utility for readers/recovery).
     */
    public static int computeCrc32(byte tomb, byte[] key, byte[] value) {
        CRC32 c = new CRC32();
        c.update(tomb);
        c.update(key);
        c.update(value);
        return (int) c.getValue();
    }

    /**
     * Total serialized size in bytes (header + payload).
     */
    public int bytes() {
        return bytes;
    }

    public boolean isTombstone() {
        return tomb == 1;
    }

    public byte tombRaw() {
        return tomb;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public int crc() {
        return crc;
    }
}
