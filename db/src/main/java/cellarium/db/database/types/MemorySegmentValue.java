package cellarium.db.database.types;

import cellarium.db.MemorySegmentComparator;
import java.lang.foreign.MemorySegment;

public final class MemorySegmentValue extends AValue<MemorySegment> {
    public MemorySegmentValue(MemorySegment value, DataType dataType, long sizeBytes) {
        super(value, dataType, sizeBytes);
    }

    @Override
    public int compareTo(AValue<MemorySegment> o) {
        return MemorySegmentComparator.INSTANCE.compare(this.value, o.value);
    }
}
