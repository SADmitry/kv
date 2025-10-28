package sdmitry.kv;

import java.util.Objects;

/**
 * Logical address of a record inside an append-only segment.
 *
 * Fields:
 *   - segmentId : monotonically increasing id (used in file name %020d.seg)
 *   - offset    : byte offset from the beginning of that segment (header start)
 *
 * Why a separate type:
 *   - Clear API: pass one value-object instead of juggling two longs.
 *   - Immutable: safe to publish into the in-memory index.
 *   - Comparable: natural order by (segmentId, offset) helps scans/debugging.
 *
 * Notes:
 *   - We don’t validate against file size here — readers do that.
 *   - No serialization persisted: index is rebuilt on startup by scanning segments.
 *   - Implemented as a Java record for lightweight value semantics.
 */
public record Position(long segmentId, long offset) implements Comparable<Position> {

    /**
     * Create a new Position.
     *
     * @param segmentId monotonically increasing segment identifier
     * @param offset byte offset within the segment where the record header begins (>= 0)
     * @throws IllegalArgumentException if {@code segmentId < 0} or {@code offset < 0}
     */
    public Position {
        if (segmentId < 0) {
            throw new IllegalArgumentException("segmentId must be >= 0");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
    }

    /**
     * Natural ordering: by {@code segmentId}, then by {@code offset}.
     */
    @Override
    public int compareTo(Position other) {
        Objects.requireNonNull(other, "other");
        int bySeg = Long.compare(this.segmentId, other.segmentId);
        return (bySeg != 0) ? bySeg : Long.compare(this.offset, other.offset);
    }
}
