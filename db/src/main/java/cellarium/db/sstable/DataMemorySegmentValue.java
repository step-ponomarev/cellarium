package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;

public final class DataMemorySegmentValue extends MemorySegmentValue {
    public DataMemorySegmentValue(MemorySegment memorySegment) {
        super(memorySegment);
    }
}
