package cellarium.disk.writer;

import cellarium.disk.AMemorySegmentHandler;
import cellarium.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentEntryWriter extends AMemorySegmentHandler implements Writer<MemorySegmentEntry> {
    public MemorySegmentEntryWriter(MemorySegment memorySegment, long tombstoneTag) {
        super(memorySegment, tombstoneTag);
    }

    @Override
    public long write(MemorySegmentEntry entry) {
        final MemorySegment key = entry.getKey();
        final long keySize = key.byteSize();

        final long startOffset = position;
        MemoryAccess.setLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER, keySize);
        position += Long.BYTES;

        memorySegment.asSlice(position, keySize).copyFrom(key);
        position += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER, entry.getTimestamp());
        position += Long.BYTES;

        final MemorySegment value = entry.getValue();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER, this.tombstoneTag);
            position += Long.BYTES;
            return position - startOffset;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER, valueSize);
        position += Long.BYTES;

        memorySegment.asSlice(position, valueSize).copyFrom(value);
        position += valueSize;

        return position - startOffset;
    }
}
