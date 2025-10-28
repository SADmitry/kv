package sdmitry.kv;

/**
 * Binary log record for the append-only storage.
 *
 * <p><strong>Wire format (big-endian):</strong></p>
 * <pre>
 * [crc32(4)][tomb(1)][klen(4)][vlen(4)][key bytes][value bytes]
 *   crc32   : CRC of (tomb, key, value) using java.util.zip.CRC32
 *   tomb    : 0 = normal PUT, 1 = tombstone (DELETE)
 *   klen    : key length in bytes (int >= 0)
 *   vlen    : value length in bytes (int >= 0); for tombstone vlen == 0
 *   key     : UTF-8 bytes of the key (caller controls encoding)
 *   value   : raw bytes of the value
 * </pre>
 *
 * <p><strong>Design notes:</strong></p>
 * <ul>
 *   <li>We keep the record <em>append-only</em> to make writes sequential and crash-friendly.
 *       Index holds only positions; recovery scans segments and rebuilds the in-memory map.</li>
 *   <li><em>CRC first</em> in the header lets us detect torn/partial writes early during recovery
 *       (a corrupted tail stops the scan without poisoning the index).</li>
 *   <li><em>Big-endian</em> is used via {@code ByteBuffer}'s default; consistent with many on-disk formats.</li>
 *   <li>We expose a compact immutable object with explicit {@code bytes()} size and a single
 *       {@code toByteBuffer()} serializer to avoid duplication in writers.</li>
 *   <li>{@code tombstone} is a single byte to keep the header small and branchless at read time.</li>
 * </ul>
 *
 * <p><strong>Why a dedicated class:</strong></p>
 * <ul>
 *   <li>Separates storage-format concerns from engine logic (SRP), making compaction/recovery code clearer.</li>
 *   <li>Allows future extensions (e.g., record versioning, checksums other than CRC32, flags) without touching the engine.</li>
 *   <li>Avoids accidental format drift when multiple components need to serialize/deserialize records.</li>
 * </ul>
 *
 * <p><strong>Naming:</strong> Although Java has {@code java.lang.Record} (for record types),
 * this class is a regular {@code final class} named {@code Record}. Using the simple name keeps
 * parity with literature (Bitcask-style “record”). If you prefer, rename to {@code KvEntry} and adjust imports.</p>
 */

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.CRC32;

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

    // Convenience helpers when code works with String keys/values.
    public static Record normalUtf8(String key, byte[] value) {
        return normal(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public static Record tombstoneUtf8(String key) {
        return tombstone(key.getBytes(StandardCharsets.UTF_8));
    }
}
