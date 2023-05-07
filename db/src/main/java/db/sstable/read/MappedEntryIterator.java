package db.sstable.read;

import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.NoSuchElementException;
import db.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public final class MappedEntryIterator implements Iterator<MemorySegmentEntry> {
    private final MemorySegment memorySegment;
    private final long tombstoneTag;
    private final ByteOrder byteOrder;
    private long position;

    public MappedEntryIterator(MemorySegment memorySegment, long tombstoneTag, ByteOrder byteOrder) {
        this.memorySegment = memorySegment;
        this.tombstoneTag = tombstoneTag;
        this.byteOrder = byteOrder;
        this.position = 0;
    }

    @Override
    public MemorySegmentEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more data, current position: " + position + " max position: " + memorySegment.byteSize());
        }

        final long keySize = MemoryAccess.getLongAtOffset(memorySegment, position, byteOrder);
        position += Long.BYTES;

        final MemorySegment key = MemorySegment.ofByteBuffer(
                memorySegment.asSlice(position, keySize).asByteBuffer()
        ).asReadOnly();
        position += keySize;

        final long timestamp = MemoryAccess.getLongAtOffset(memorySegment, position, byteOrder);
        position += Long.BYTES;

        final long valueSize = MemoryAccess.getLongAtOffset(memorySegment, position, byteOrder);
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
        return position < memorySegment.byteSize();
    }
}
