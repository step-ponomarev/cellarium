package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.comparator.AMemorySegmentComparator;
import cellarium.db.comparator.ComparatorFactory;
import cellarium.db.converter.sstable.SSTableKey;

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

    public MemorySegment getDataRange(SSTableKey from, SSTableKey to) {
        final MemorySegment dataSegment = dataSegmentValue.getMemorySegment();
        if (from == null && to == null) {
            return dataSegment;
        }

        final AMemorySegmentComparator comparator = from == null
                ? ComparatorFactory.getComparator(to.type)
                : ComparatorFactory.getComparator(from.type);
        final MemorySegment indexMemorySegment = indexSegmentValue.getMemorySegment();
        if (from == null) {
            int i = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, to.getMemorySegment(), comparator);
            if (i < 0) {
                i = Math.abs(i) - 1;
            }

            final long offset = indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);
            //todo: не хранить офсеты в массиве!
            return dataSegment.asSlice(0, offset);
        }

        if (to == null) {
            final int i = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, from.getMemorySegment(), comparator);
            final long offset = indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);

            return dataSegment.asSlice(offset);
        }

        //TODO: если ключи равны - вернуть одно значение
        final int iFrom = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, from.getMemorySegment(), comparator);
        final int iTo = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, to.getMemorySegment(), comparator);

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
