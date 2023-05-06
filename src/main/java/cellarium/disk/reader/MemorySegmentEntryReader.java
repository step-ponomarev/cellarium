package cellarium.disk.reader;

import cellarium.disk.AMemorySegmentHandler;
import cellarium.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentEntryReader extends AMemorySegmentHandler implements Reader<MemorySegmentEntry> {
    public MemorySegmentEntryReader(MemorySegment memorySegment, long tombstoneTag) {
        super(memorySegment, tombstoneTag);
    }

    @Override
    public MemorySegmentEntry read() {
        final long keySize = MemoryAccess.getLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER);
        position += Long.BYTES;

        final MemorySegment key = MemorySegment.ofByteBuffer(
                memorySegment.asSlice(position, keySize).asByteBuffer()
        ).asReadOnly();
        position += keySize;

        final long timestamp = MemoryAccess.getLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER);
        position += Long.BYTES;

        final long valueSize = MemoryAccess.getLongAtOffset(memorySegment, position, STANDART_BYTE_OREDER);
        position += Long.BYTES;

        final boolean entryIsTombstone = valueSize == this.tombstoneTag;
        if (entryIsTombstone) {
            return new MemorySegmentEntry(key, null, timestamp);
        }

        final MemorySegment value = MemorySegment.ofByteBuffer(
                memorySegment.asSlice(position, valueSize).asByteBuffer()
        ).asReadOnly();
        position += valueSize;

        return new MemorySegmentEntry(key, value, timestamp);
    }

    @Override
    public boolean hasNext() {
        return memorySegment.byteSize() != position;
    }
}
