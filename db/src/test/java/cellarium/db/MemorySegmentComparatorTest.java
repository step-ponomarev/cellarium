package cellarium.db;

import jdk.incubator.foreign.MemorySegment;
import org.junit.Assert;
import org.junit.Test;

public final class MemorySegmentComparatorTest {
    private static final MemorySegmentComparator COMPARATOR = MemorySegmentComparator.INSTANCE;

    @Test
    public void testCompareEquals() {
        final MemorySegment firstValue = MemorySegmentUtils.stringToMemorySegment("str");
        final MemorySegment secondValue = MemorySegmentUtils.stringToMemorySegment("str");

        Assert.assertEquals(0, COMPARATOR.compare(firstValue, secondValue));
    }

    @Test
    public void testCompareDifferent() {
        final String firstString = "str1";
        final String secondString = "str2";

        final int expected = firstString.compareTo(secondString);

        final MemorySegment firstValue = MemorySegmentUtils.stringToMemorySegment(firstString);
        final MemorySegment secondValue = MemorySegmentUtils.stringToMemorySegment(secondString);

        Assert.assertEquals(expected < 0, COMPARATOR.compare(firstValue, secondValue) < 0);
    }

    @Test
    public void testCompareEqualsStart() {
        final String firstString = "str1";
        final String secondString = "str1234567910111213";

        final int expected = firstString.compareTo(secondString);

        final MemorySegment firstValue = MemorySegmentUtils.stringToMemorySegment(firstString);
        final MemorySegment secondValue = MemorySegmentUtils.stringToMemorySegment(secondString);

        Assert.assertEquals(expected < 0, COMPARATOR.compare(firstValue, secondValue) < 0);
    }

    @Test
    public void testSymmetry() {
        final String firstValue = "str1";
        final String secondValue = "str2";

        final MemorySegment firstMem = MemorySegmentUtils.stringToMemorySegment(firstValue);
        final MemorySegment secondMem = MemorySegmentUtils.stringToMemorySegment(secondValue);

        final int firstToSecondCompareResult = COMPARATOR.compare(firstMem, secondMem);
        final int secondToFirstCompareResult = COMPARATOR.compare(secondMem, firstMem);

        Assert.assertNotEquals(firstToSecondCompareResult, secondToFirstCompareResult);
    }

    @Test
    public void testReflection() {
        final String value = "str1";

        final MemorySegment firstValue = MemorySegmentUtils.stringToMemorySegment(value);
        final MemorySegment secondValue = MemorySegmentUtils.stringToMemorySegment(value);

        Assert.assertEquals(COMPARATOR.compare(firstValue, secondValue), COMPARATOR.compare(secondValue, firstValue));
    }

    @Test
    public void testTransitivity() {
        final MemorySegment firstValue = MemorySegmentUtils.stringToMemorySegment("str1");
        final MemorySegment secondValue = MemorySegmentUtils.stringToMemorySegment("str2");
        final MemorySegment lastValue = MemorySegmentUtils.stringToMemorySegment("str3");

        final int firstToSecondCompareResult = COMPARATOR.compare(firstValue, secondValue);
        final int secondToLastCompareResult = COMPARATOR.compare(secondValue, lastValue);
        final int firstToLastCompareResult = COMPARATOR.compare(secondValue, lastValue);

        Assert.assertTrue(firstToLastCompareResult < 0);
        Assert.assertTrue(secondToLastCompareResult < 0);
        Assert.assertTrue(firstToSecondCompareResult < 0);
    }
}
