package cellarium.dao.disk.writer;

import cellarium.dao.disk.AMemorySegmentHandler;
import cellarium.dao.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentEntryWriter extends AMemorySegmentHandler {
    public MemorySegmentEntryWriter(MemorySegment memorySegment, long tombstoneTag) {
        super(memorySegment, tombstoneTag);
    }

    public long write(MemorySegmentEntry entry) {
        final MemorySegment key = entry.getKey();
        final long keySize = key.byteSize();

        final long startOffset = position;
        MemoryAccess.setLongAtOffset(memorySegment, position, keySize);
        position += Long.BYTES;

        memorySegment.asSlice(position, keySize).copyFrom(key);
        position += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, position, entry.getTimestamp());
        position += Long.BYTES;

        final MemorySegment value = entry.getValue();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, position, this.tombstoneTag);
            position += Long.BYTES;
            return position - startOffset;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, position, valueSize);
        position += Long.BYTES;

        memorySegment.asSlice(position, valueSize).copyFrom(value);
        position += valueSize;

        return position - startOffset;
    }
}
