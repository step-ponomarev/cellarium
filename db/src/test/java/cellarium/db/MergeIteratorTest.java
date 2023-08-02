package cellarium.db;

import cellarium.db.storage.MergeIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public final class MergeIteratorTest {

    @Test
    public void testMergeTwoIterators() {
        final List<Integer> list1 = Arrays.asList(1, 3, 5);
        final List<Integer> list2 = Arrays.asList(2, 4, 6);

        final Comparator<Integer> comparator = Comparator.naturalOrder();
        final Iterator<Integer> mergedIterator = MergeIterator.of(List.of(list1.iterator(), list2.iterator()), comparator);

        final List<Integer> expectedList = Arrays.asList(1, 2, 3, 4, 5, 6);
        equals(expectedList, mergedIterator);
    }

    @Test
    public void testMergeMultipleIterators() {
        final List<Integer> list1 = Arrays.asList(1, 4, 7);
        final List<Integer> list2 = Arrays.asList(2, 5, 8);
        final List<Integer> list3 = Arrays.asList(3, 6, 9);

        final Comparator<Integer> comparator = Comparator.naturalOrder();
        final Iterator<Integer> mergedIterator = MergeIterator.of(
                List.of(list1.iterator(), list2.iterator(), list3.iterator()),
                comparator
        );

        final List<Integer> expectedList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        equals(expectedList, mergedIterator);
    }

    @Test
    public void testMergeDuplicated() {
        final List<Integer> list1 = Arrays.asList(1, 2, 5);
        final List<Integer> list2 = Arrays.asList(1, 3, 6);
        final List<Integer> list3 = Arrays.asList(1, 2, 3);

        final Comparator<Integer> comparator = Comparator.naturalOrder();
        final Iterator<Integer> mergedIterator = MergeIterator.of(
                List.of(list1.iterator(), list2.iterator(), list3.iterator()),
                comparator
        );

        final List<Integer> expectedList = Arrays.asList(1, 2, 3, 5, 6);
        equals(expectedList, mergedIterator);
    }

    @Test
    public void testMergeEmptyIterators() {
        final Comparator<Integer> comparator = Comparator.naturalOrder();
        final Iterator<Integer> mergedIterator = MergeIterator.of(
                List.of(Collections.emptyIterator(), Collections.emptyIterator(), Collections.emptyIterator()),
                comparator
        );

        Assert.assertFalse(mergedIterator.hasNext());
    }

    private static void equals(List<Integer> values, Iterator<Integer> iter) {
        if (values.isEmpty() && iter.hasNext()) {
            Assert.fail();
        }

        for (Integer v : values) {
            Assert.assertEquals(v, iter.next());
        }
    }
}
