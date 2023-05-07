package cellarium.db.sstable;

import java.io.Closeable;
import java.nio.ByteOrder;
import cellarium.db.sstable.read.MappedEntryIterator;
import cellarium.db.entry.EntryComparator;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

final class Index implements Closeable {
    private final MemorySegment indexMemorySegment;
    private final ByteOrder byteOrder;

    private final int maxIndex;

    public Index(MemorySegment indexMemorySegment, ByteOrder byteOrder) {
        this.indexMemorySegment = indexMemorySegment;
        this.byteOrder = byteOrder;
        this.maxIndex = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;
    }

    public long getEntryOffsetByIndex(int index) {
        return MemoryAccess.getLongAtIndex(indexMemorySegment, index, byteOrder);
    }

    public int findIndexOfKey(MemorySegment key, MemorySegment tableMemorySegment) {
        if (indexMemorySegment == null || tableMemorySegment == null || key == null) {
            throw new NullPointerException("Arguments cannot be null!");
        }

        int low = 0;
        int high = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            final long keyPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, mid, byteOrder);

            /**
             * Специфика записи данных.
             * см {@link MemorySegmentEntryWriter} и {@link MappedEntryIterator}
             */
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition, byteOrder);
            final MemorySegment current = tableMemorySegment.asSlice(keyPosition + Long.BYTES, keySize);

            final int compareResult = EntryComparator.compareMemorySegments(current, key);
            if (compareResult < 0) {
                low = mid + 1;
            } else if (compareResult > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -low;
    }

    public int getMaxIndex() {
        return maxIndex;
    }

    @Override
    public void close() {
        indexMemorySegment.scope().close();
    }
}
