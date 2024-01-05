package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;

public class MemorySegmentValue {
    private final MemorySegment memorySegment;

    public MemorySegmentValue(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
    }

    public final MemorySegment getMemorySegment() {
        return memorySegment;
    }
}
