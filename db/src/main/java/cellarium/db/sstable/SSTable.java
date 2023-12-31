package cellarium.db.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

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

        //TODO: Это совершенно не будет работать, ибо если мы не находим точного ключа, то нам нужно взять следующий.
        if (from == null) {
            final int i = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, to);
            return dataSegment.asSlice(0, indexOffsets[i]);
        }

        if (to == null) {
            final int i = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, from);
            return dataSegment.asSlice(indexOffsets[i]);
        }

        //todo: fix it, incorrect
        int iFrom = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, from);
        if (iFrom < 0) {
            iFrom = correctFromIndex(iFrom);
        }

        int iTo = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, to);
        if (iTo < 0) {
            iTo = correctFromIndex(iTo);
        }

        return dataSegment.asSlice(indexOffsets[iFrom], indexOffsets[iTo]);
    }

    private int correctFromIndex(int i) {
        int index = -i;

        long indexOffset = indexOffsets[index];
        
    }

    private int correctToIndex(int i) {

    }

    @Override
    public void close() throws IOException {
//        this.indexSegment.scope().close();
//        this.dataSegment.scope().close();
    }
}
