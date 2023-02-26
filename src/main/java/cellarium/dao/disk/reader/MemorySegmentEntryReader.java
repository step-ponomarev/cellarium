package cellarium.dao.disk.reader;

import cellarium.dao.disk.AMemorySegmentHandler;
import cellarium.dao.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentEntryReader extends AMemorySegmentHandler implements Reader<MemorySegmentEntry> {
    public MemorySegmentEntryReader(MemorySegment memorySegment, long tombstoneTag) {
        super(memorySegment, tombstoneTag);
    }

    @Override
    public MemorySegmentEntry read() {
        final long keySize = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        // Аллоцирум чтобы при дальнейшем взаимодействии с entry не было привязки к первичному scope
        final MemorySegment key = MemorySegment.ofByteBuffer(memorySegment.asSlice(position, keySize).asByteBuffer());
        position += keySize;

        final long timestamp = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        final long valueSize = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        final boolean entryIsTombstone = valueSize == this.tombstoneTag;
        if (entryIsTombstone) {
            return new MemorySegmentEntry(key, null, timestamp);
        }

        // Аллоцирум чтобы при дальнейшем взаимодействии с entry не было привязки к первичному scope
        final MemorySegment value = MemorySegment.ofByteBuffer(
                memorySegment.asSlice(position, valueSize).asByteBuffer()
        );
        position += valueSize;

        return new MemorySegmentEntry(key, value, timestamp);
    }

    @Override
    public boolean hasNext() {
        return memorySegment.byteSize() != position;
    }
}
