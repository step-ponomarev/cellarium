package cellarium.dao.sstable;

import java.io.Closeable;
import cellarium.dao.entry.EntryComparator;
import cellarium.dao.disk.reader.MemorySegmentEntryReader;
import cellarium.dao.disk.writer.MemorySegmentEntryWriter;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

final class Index implements Closeable {
    private final MemorySegment tableMemorySegment;
    private final MemorySegment indexMemorySegment;

    public Index(MemorySegment tableMemorySegment, MemorySegment indexMemorySegment) {
        this.tableMemorySegment = tableMemorySegment;
        this.indexMemorySegment = indexMemorySegment;
    }

    public int findIndexOfKey(MemorySegment key) {
        if (indexMemorySegment == null || tableMemorySegment == null || key == null) {
            throw new NullPointerException("Arguments cannot be null!");
        }

        int low = 0;
        int high = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            final long keyPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, mid);

            /**
             * Специфика записи данных.
             * см {@link MemorySegmentEntryWriter} и {@link MemorySegmentEntryReader}
             */
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition);
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
        return (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;
    }

    public int getFromIndex(MemorySegment from) {
        if (from == null) {
            return 0;
        }

        return Math.abs(
                findIndexOfKey(from)
        );
    }

    public int getToIndex(MemorySegment to) {
        if (to == null) {
            return getMaxIndex() + 1;
        }

        return Math.abs(findIndexOfKey(to));
    }

    public long getEntryPositionByIndex(int index) {
        return MemoryAccess.getLongAtIndex(indexMemorySegment, index);
    }

    @Override
    public void close() {
        indexMemorySegment.unload();
        indexMemorySegment.force();
        indexMemorySegment.scope().close();
    }
}
