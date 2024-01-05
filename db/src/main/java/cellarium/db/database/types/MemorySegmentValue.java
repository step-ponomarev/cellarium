package cellarium.db.database.types;

import cellarium.db.comparator.AMemorySegmentComparator;

import java.lang.foreign.MemorySegment;

public final class MemorySegmentValue extends AValue<MemorySegment> {
    private final AMemorySegmentComparator comarator;

    public MemorySegmentValue(MemorySegment value, DataType dataType, long sizeBytes, AMemorySegmentComparator comparator) {
        super(value, dataType, sizeBytes);
        this.comarator = comparator;
    }

    @Override
    public int compareTo(AValue<MemorySegment> o) {
        return this.comarator.compare(this.value, o.value);
    }
}
