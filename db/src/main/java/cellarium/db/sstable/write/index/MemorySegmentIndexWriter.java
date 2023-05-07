package cellarium.db.sstable.write.index;

import java.io.IOException;
import java.nio.ByteOrder;
import cellarium.db.sstable.write.AWriter;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentIndexWriter extends AWriter<Long> {
    private final MemorySegment memorySegment;

    public MemorySegmentIndexWriter(MemorySegment memorySegment, ByteOrder byteOrder) {
        super(byteOrder);
        this.memorySegment = memorySegment;
    }

    @Override
    public long write(Long value) throws IOException {
        MemoryAccess.setLongAtOffset(memorySegment, position, byteOrder, value);
        position += Long.BYTES;

        return Long.BYTES;
    }
}
