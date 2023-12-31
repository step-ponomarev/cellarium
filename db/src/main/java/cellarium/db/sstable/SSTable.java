package cellarium.db.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

import cellarium.db.MemorySegmentComparator;
import cellarium.db.MemorySegmentUtils;

public final class SSTable implements Closeable {
    // четко знает свой размер в байтах
    // есть метаданные
    // есть индекс

    private final MemorySegment indexSegment; // key, entity offset
    private final long[] indexOffsets;

    private final MemorySegment dataSegment;

    SSTable(MemorySegment indexSegment, long[] indexOffsets, MemorySegment dataSegment) {
        this.indexSegment = indexSegment;
        this.indexOffsets = indexOffsets;

        this.dataSegment = dataSegment;
    }

    // Должна уметь искать по ключу.
    // Ключ - есть тип и значение
    public MemorySegment getDataRange(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return dataSegment;
        }

        if (from == null) {
            final int i = findIndexOfKey(indexSegment, indexOffsets, to, false);
            //TODO: out of bound
            return dataSegment.asSlice(0, indexOffsets[i + 1]);
        }

        if (to == null) {
            final int i = findIndexOfKey(indexSegment, indexOffsets, from, true);
            return dataSegment.asSlice(indexOffsets[i]);
        }

        int iFrom = findIndexOfKey(indexSegment, indexOffsets, from, true);
        int iTo = findIndexOfKey(indexSegment, indexOffsets, to, false);

        //TODO: out of bound
        return dataSegment.asSlice(indexOffsets[iFrom], indexOffsets[iTo + 1]);
    }

    /**
     * @param indexSegment
     * @param key
     * @return index of offset for index memory segment if found, otherwise negative index
     */
    static int findIndexOfKey(MemorySegment indexSegment, long[] indexOffsets, MemorySegment key, boolean from) {
        int left = 0;
        int right = indexOffsets.length - 1;

        while (left <= right) {
            final int i = (left + right) >>> 1;
            final MemorySegment current = MemorySegmentUtils.readValue(indexSegment, indexOffsets[i]);
            final int compare = MemorySegmentComparator.INSTANCE.compare(current, key);
            if (compare > 0) {
                right = i - 1;
                continue;
            }

            if (compare < 0) {
                left = i + 1;
                continue;
            }

            return i;
        }

        return left;
    }

    @Override
    public void close() throws IOException {
//        this.indexSegment.scope().close();
//        this.dataSegment.scope().close();
    }
}
