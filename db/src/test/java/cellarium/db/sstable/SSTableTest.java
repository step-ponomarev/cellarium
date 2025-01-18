package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.comparator.AMemorySegmentComparator;
import cellarium.db.MemorySegmentUtils;
import cellarium.db.comparator.ComparatorFactory;
import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.converter.sstable.SSTableKey;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import utils.SSTableTestUtils;
import utils.TestData;

public class SSTableTest {

    @Test
    public void testSingleValue() {
        final SSTable ssTable = flushSStable(1);
        MemorySegment dataRange = ssTable.getDataRange(
                new SSTableKey(
                        SSTableValueConverter.INSTANCE.convert(IntegerValue.of(0)),
                        DataType.INTEGER
                ),
                new SSTableKey(
                        SSTableValueConverter.INSTANCE.convert(IntegerValue.of(0)),
                        DataType.INTEGER
                )
        );

        final MemorySegment memorySegment = MemorySegmentUtils.sliceFirstDbValue(dataRange);
        AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(memorySegment);
        Assert.assertEquals(IntegerValue.of(0).getValue(), aValue.getValue());
    }

    @Test
    public void testFullRange() {
        final TestData testData = SSTableTestUtils.mockIntListData(300);
        final SSTable ssTable = new SSTable(testData.data, testData.index);

        final AMemorySegmentComparator comparator = ComparatorFactory.getComparator(DataType.INTEGER);

        final int compare = comparator.compare(
                testData.data.getMemorySegment(),
                ssTable.getDataRange(null, null)
        );

        Assert.assertEquals(0, compare);
    }

    @Test
    public void testFromStartToMiddle() {
        final int amount = 300;
        final TestData testData = SSTableTestUtils.mockIntListData(amount);
        final SSTable ssTable = new SSTable(testData.data, testData.index);

        final int valueAmount = amount / 2;
        final MemorySegment toValue = SSTableValueConverter.INSTANCE.convert(
                IntegerValue.of(valueAmount)
        );

        final MemorySegment dataRange = ssTable.getDataRange(null, new SSTableKey(toValue, DataType.INTEGER));

        long offset = 0;
        MemorySegment currentSlice;
        for (int i = 0; i < valueAmount; i++) {
            currentSlice = dataRange.asSlice(offset);
            MemorySegment memorySegment = MemorySegmentUtils.sliceFirstDbValue(currentSlice);
            offset += memorySegment.byteSize();

            final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(memorySegment);
            Assert.assertEquals(i, aValue.getValue());
        }

        Assert.assertEquals(dataRange.byteSize(), offset);
    }

    @Test
    public void testFromMiddleToEnd() {
        final int amount = 300;
        final TestData testData = SSTableTestUtils.mockIntListData(amount);
        final SSTable ssTable = new SSTable(testData.data, testData.index);

        final int valueAmount = amount / 2;
        final MemorySegment from = SSTableValueConverter.INSTANCE.convert(IntegerValue.of(valueAmount));
        final MemorySegment dataRange = ssTable.getDataRange(
                new SSTableKey(from, DataType.INTEGER), null
        );

        long offset = 0;
        MemorySegment currentSlice;
        for (int i = valueAmount; i < amount; i++) {
            currentSlice = dataRange.asSlice(offset);
            MemorySegment memorySegment = MemorySegmentUtils.sliceFirstDbValue(currentSlice);
            offset += memorySegment.byteSize();

            final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(memorySegment);
            Assert.assertEquals(i, aValue.getValue());
        }

        Assert.assertEquals(dataRange.byteSize(), offset);
    }

    @Test
    public void testSingleAllSingleValue() {
        final int amount = 300;
        final TestData testData = SSTableTestUtils.mockIntListData(amount);
        final SSTable ssTable = new SSTable(testData.data, testData.index);

        int totalOffset = 0;
        for (int i = 0; i < amount; i++) {
            final MemorySegment key = SSTableValueConverter.INSTANCE.convert(IntegerValue.of(i));
            final SSTableKey ssTableKey = new SSTableKey(key, DataType.INTEGER);
            final MemorySegment value = ssTable.getDataRange(ssTableKey, ssTableKey);
            totalOffset += value.byteSize();

            final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(value);
            Assert.assertEquals(i, aValue.getValue());
        }

        final MemorySegment allValues = ssTable.getDataRange(null, null);
        Assert.assertEquals(allValues.byteSize(), totalOffset);
    }

    private static SSTable flushSStable(int amount) {
        final TestData data = SSTableTestUtils.mockIntListData(amount);

        return new SSTable(
                data.data,
                data.index
        );
    }
}
