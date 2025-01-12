package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;

public final class IndexMemorySegmentValue extends MemorySegmentValue {
    public final int maxOffsetIndex;

    public IndexMemorySegmentValue(MemorySegment memorySegment) {
        super(memorySegment);
        this.maxOffsetIndex = (int) (memorySegment.byteSize() / Long.BYTES) - 1;
    }
}
