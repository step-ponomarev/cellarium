package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.comparator.ComparatorFactory;
import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import utils.SSTableTestUtils;
import utils.TestData;

public class MemorySegmentBinarySearchTest {
    private final Comparator<MemorySegment> INT_MEMORY_SEGMENT_COMPARATOR = ComparatorFactory.getComparator(DataType.INTEGER);

    @Test
    public void testSingleKey() {
        final TestData index = SSTableTestUtils.mockIntListData(1);

        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                index.data,
                index.index,
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(0)),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(0, foundIndex);
    }

    @Test
    public void testFindExactKey() {
        final TestData index = SSTableTestUtils.mockIntListData(3);

        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                index.data,
                index.index,
                index.keys.get(2),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(2, foundIndex);
    }

    @Test
    public void testFindPositionWithHole() {
        // 0 2 3
        final TestData data = SSTableTestUtils.mockIntListData(4, Set.of(1));

        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                data.data,
                data.index,
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(1)),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(-1, foundIndex);
    }

    @Test
    public void testFindPositionWithTwoHoles() {
        // 0 1 2 5
        final TestData data = SSTableTestUtils.mockIntListData(5, Set.of(3, 4));
        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                data.data,
                data.index,
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(4)),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(-3, foundIndex);
    }

    @Test
    public void testFindPositionInLargeData() {
        final TestData data = SSTableTestUtils.mockIntListData(300);
        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                data.data,
                data.index,
                data.keys.get(5),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(5, foundIndex);
    }

    @Test
    public void testFindPositionBiggestThanMaximumData() {
        // [0, 299]
        final TestData data = SSTableTestUtils.mockIntListData(300);
        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                data.data,
                data.index,
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(300)),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(-300, foundIndex);
    }

    @Test
    public void testFindPositionLessThanMinimumData() {
        // [6, 299]
        final TestData data = SSTableTestUtils.mockIntListData(300, Set.of(0, 1, 2, 3, 4, 5));
        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                data.data,
                data.index,
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(2)),
                INT_MEMORY_SEGMENT_COMPARATOR
        );

        Assert.assertEquals(0, foundIndex);
    }
}
