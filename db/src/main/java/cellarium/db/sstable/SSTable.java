package cellarium.db.sstable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import cellarium.db.MemorySegmentUtils;

public final class SSTable {
    // четко знает свой размер в байтах
    // есть метаданные
    // есть индекс

    private final DataMemorySegmentValue dataSegmentValue;
    private final IndexMemorySegmentValue indexSegmentValue; // key, entity offset

    SSTable(DataMemorySegmentValue dataSegment, IndexMemorySegmentValue indexSegment) {
        this.indexSegmentValue = indexSegment;
        this.dataSegmentValue = dataSegment;
    }

    // Должна уметь искать по ключу.
    // Ключ - есть тип и значение
    public MemorySegment getDataRange(MemorySegment from, MemorySegment to) {
        final MemorySegment dataSegment = dataSegmentValue.getMemorySegment();
        if (from == null && to == null) {
            return dataSegment;
        }

        final MemorySegment indexMemorySegment = indexSegmentValue.getMemorySegment();
        if (from == null) {
            int i = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, to);
            if (i < 0) {
                i = Math.abs(i) - 1;
            }

            final long offset = indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);
            //todo: не хранить офсеты в массиве!
            return dataSegment.asSlice(0, offset);
        }

        if (to == null) {
            final int i = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, from);
            final long offset = indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);

            return dataSegment.asSlice(offset);
        }

        //TODO: если ключи равны - вернуть одно значение
        final int iFrom = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, from);
        final int iTo = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, to);

        final long fromOffset = MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iTo);
        if (iTo == indexSegmentValue.maxOffsetIndex) {
            return dataSegment.asSlice(fromOffset);
        }

        final long size = iFrom == iTo
                ? MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iFrom + 1) - MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iFrom)
                : MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iTo) - fromOffset;

        return dataSegment.asSlice(fromOffset, size);
    }
}
