package sdmitry.kv;

import java.util.Objects;

/**
 * Immutable logical address of a record inside an append-only log segment.
 *
 * <p>A {@code Position} identifies a record by:
 * <ul>
 *   <li>{@code segmentId} – monotonically increasing identifier of the segment file
 *       (e.g. used in filename {@code %020d.seg});</li>
 *   <li>{@code offset} – byte offset from the beginning of that segment where the record header starts.</li>
 * </ul>
 *
 * <h2>Why a dedicated type</h2>
 * <ul>
 *   <li><strong>Clarity &amp; safety:</strong> separates the notion of “where on disk” from the higher-level
 *       engine logic. Passing a single value object is less error-prone than juggling two longs.</li>
 *   <li><strong>Immutability:</strong> positions must be stable once published to the in-memory index;
 *       a record type guarantees that.</li>
 *   <li><strong>Ordering:</strong> {@link Comparable} allows natural ordering (segment, then offset) useful for
 *       debugging, scans, or future compaction heuristics.</li>
 * </ul>
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li>Position is <em>opaque</em> to callers; it is not validated against actual file size on creation
 *       (validation belongs to readers).</li>
 *   <li>No serialization is provided here on purpose: the in-memory index is rebuilt on startup by scanning
 *       segments, so we avoid persisting Position structures.</li>
 *   <li>Using a Java <em>record</em> (Java 21) gives value-semantics, generated {@code equals/hashCode/toString},
 *       and keeps the type lightweight.</li>
 * </ul>
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

    /**
     * Convenience helper to create a new position within the same segment, at a different offset.
     * Useful when iterating records inside the same segment.
     */
    public Position withOffset(long newOffset) {
        return new Position(this.segmentId, newOffset);
    }
}
