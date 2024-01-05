package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;

import org.junit.Assert;
import org.junit.Test;

abstract class AComparatorTest {
    private final AMemorySegmentComparator COMPARATOR;

    public AComparatorTest(AMemorySegmentComparator COMPARATOR) {
        this.COMPARATOR = COMPARATOR;
    }

    @Test
    public void testCompareEquals() {
        final MemorySegment firstValue = createValue1();
        final MemorySegment secondValue = createValue1();

        Assert.assertEquals(0, COMPARATOR.compare(firstValue, secondValue));
    }

    @Test
    public void testCompareDifferent() {
        final MemorySegment firstValue = createValue1();
        final MemorySegment secondValue = createValue2();

        Assert.assertTrue(COMPARATOR.compare(firstValue, secondValue) < 0);
    }

    @Test
    public void testSymmetry() {
        final MemorySegment firstValue = createValue1();
        final MemorySegment secondValue = createValue2();


        Assert.assertTrue(COMPARATOR.compare(firstValue, secondValue) < 0);
        Assert.assertTrue(COMPARATOR.compare(secondValue, firstValue) > 0);
    }

    @Test
    public void testReflection() {
        final MemorySegment firstValue = createValue1();
        final MemorySegment secondValue = createValue1();

        Assert.assertEquals(COMPARATOR.compare(firstValue, secondValue), COMPARATOR.compare(secondValue, firstValue));
    }

    @Test
    public void testTransitivity() {
        final MemorySegment firstValue = createValue1();
        final MemorySegment secondValue = createValue2();
        final MemorySegment lastValue = createValue3();

        final int firstToSecondCompareResult = COMPARATOR.compare(firstValue, secondValue);
        final int secondToLastCompareResult = COMPARATOR.compare(secondValue, lastValue);
        final int firstToLastCompareResult = COMPARATOR.compare(secondValue, lastValue);

        Assert.assertTrue(firstToLastCompareResult < 0);
        Assert.assertTrue(secondToLastCompareResult < 0);
        Assert.assertTrue(firstToSecondCompareResult < 0);
    }

    public abstract MemorySegment createValue1();

    public abstract MemorySegment createValue2();

    public abstract MemorySegment createValue3();
}
