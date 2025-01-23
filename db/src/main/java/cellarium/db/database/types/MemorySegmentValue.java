package cellarium.db.database.types;

import cellarium.db.comparator.AMemorySegmentComparator;

import java.lang.foreign.MemorySegment;

public final class MemorySegmentValue extends AValue<MemorySegment> {
    private final AMemorySegmentComparator comparator;

    public MemorySegmentValue(MemorySegment value, DataType dataType, long sizeBytes, AMemorySegmentComparator comparator) {
        super(value, dataType, sizeBytes);
        this.comparator = comparator;
    }

    @Override
    public int compareTo(AValue<MemorySegment> o) {
        return this.comparator.compare(this.value, o.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
