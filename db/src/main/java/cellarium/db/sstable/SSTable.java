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

        if (from == null) {
            int i = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, to);
            if (i < 0) {
                i = Math.abs(i) - 1;
            }

            return dataSegment.asSlice(0, indexOffsets[i + 1]);
        }

        if (to == null) {
            final int i = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, from);
            return dataSegment.asSlice(indexOffsets[i]);
        }

        int iFrom = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, from);
        int iTo = MemorySegmentUtils.findIndexOfKey(indexSegment, indexOffsets, to);

        //TODO: out of bound
        return dataSegment.asSlice(indexOffsets[iFrom], indexOffsets[iTo + 1]);
    }

    @Override
    public void close() throws IOException {
//        this.indexSegment.scope().close();
//        this.dataSegment.scope().close();
    }
}
