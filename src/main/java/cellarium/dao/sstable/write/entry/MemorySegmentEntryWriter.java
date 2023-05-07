package cellarium.dao.sstable.write.entry;

import java.nio.ByteOrder;
import cellarium.dao.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentEntryWriter extends AMemorySegmentEntryWriter {
    private final MemorySegment memorySegment;

    public MemorySegmentEntryWriter(MemorySegment memorySegment, long tombstoneTag, ByteOrder byteOrder) {
        super(tombstoneTag, byteOrder);
        this.memorySegment = memorySegment;
    }

    public long write(MemorySegmentEntry entry) {
        final MemorySegment key = entry.getKey();
        final long keySize = key.byteSize();

        final long startOffset = position;
        MemoryAccess.setLongAtOffset(memorySegment, position, byteOrder, keySize);
        position += Long.BYTES;

        memorySegment.asSlice(position, keySize).copyFrom(key);
        position += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, position, byteOrder, entry.getTimestamp());
        position += Long.BYTES;

        final MemorySegment value = entry.getValue();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, position, byteOrder, this.tombstoneTag);
            position += Long.BYTES;
            return position - startOffset;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, position, byteOrder, valueSize);
        position += Long.BYTES;

        memorySegment.asSlice(position, valueSize).copyFrom(value);
        position += valueSize;

        return position - startOffset;
    }
}
