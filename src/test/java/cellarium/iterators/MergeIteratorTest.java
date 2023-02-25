package cellarium.iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import cellarium.entry.AbstractEntry;
import cellarium.entry.Entry;
import cellarium.entry.EntryComparator;
import cellarium.entry.MemorySegmentEntry;
import test.entry.EntryGeneratorList;
import test.entry.NullEntryGeneratorList;
import test.entry.TestUtils;

public class MergeIteratorTest {
    @Test
    public void emptyIteratorsTest() {
        final List<Iterator<MemorySegmentEntry>> emptyIterators = List.of(
                Collections.emptyIterator(),
                Collections.emptyIterator()
        );

        final Iterator<MemorySegmentEntry> of = MergeIterator.of(
                emptyIterators,
                EntryComparator::compareMemorySegmentEntryKeys
        );

        Assert.assertFalse(of.hasNext());
    }

    @Test
    public void equalsMergeTest() {
        final int finalCount = 10_000;

        final List<Iterator<Entry<String>>> iterators = List.of(
                new EntryGeneratorList(finalCount).iterator(),
                new EntryGeneratorList(finalCount).iterator()
        );

        final Iterator<Entry<String>> iterator = MergeIterator.of(
                iterators,
                EntryComparator::compareStringEntries
        );

        for (int i = 0; i < finalCount; i++) {
            Assert.assertTrue(iterator.hasNext());
            iterator.next();
        }

        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void mergeReplaceTest() {
        final int finalCount = 10_000;

        final String keyPrefix = "KEY_";
        final String firstPrefixValue = "FIRST_";
        final String secondPrefixValue = "SECOND_";

        final List<Iterator<Entry<String>>> iterators = List.of(
                new EntryGeneratorList(finalCount, keyPrefix, firstPrefixValue).iterator(),
                new EntryGeneratorList(finalCount, keyPrefix, secondPrefixValue).iterator()
        );

        final Iterator<Entry<String>> iterator = MergeIterator.of(
                iterators,
                EntryComparator::compareStringEntries
        );

        while (iterator.hasNext()) {
            final Entry<String> next = iterator.next();

            Assert.assertFalse(next.getValue().startsWith(firstPrefixValue));
            Assert.assertTrue(next.getValue().startsWith(secondPrefixValue));
        }
    }

    @Test
    public void nullMergeTest() {
        final int count = 10_000;
        final Iterator<Entry<String>> iterator = MergeIterator.of(
                Collections.singletonList(new NullEntryGeneratorList(count).iterator()),
                EntryComparator::compareStringEntries
        );

        for (int i = 0; i < count; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertTrue(iterator.next().getValue() == null);
        }

        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void mergeNullTest() {
        final int notNullCount = 10_000;
        final int nullCount = 2_000;

        final Iterator<Entry<String>> iterator = MergeIterator.of(
                List.of(
                        new EntryGeneratorList(notNullCount).iterator(),
                        new NullEntryGeneratorList(nullCount, "NULL_").iterator()
                ),
                EntryComparator::compareStringEntries
        );

        final int totalCount = notNullCount + nullCount;
        for (int i = 0; i < totalCount; i++) {
            Assert.assertTrue(iterator.hasNext());
            iterator.next();
        }

        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testNullReplace() {
        final int count = 10_000;

        final Iterator<Entry<String>> iterator = MergeIterator.of(
                List.of(
                        new EntryGeneratorList(count).iterator(),
                        new NullEntryGeneratorList(count).iterator()
                ),
                EntryComparator::compareStringEntries
        );

        for (int i = 0; i < count; i++) {
            Assert.assertTrue(iterator.hasNext());
            Entry<String> next = iterator.next();
            Assert.assertTrue(next.getValue() == null);
        }

        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testOrger() {
        final int count = 100;


        final List<Entry<String>> fromTailEntries = new ArrayList<>();
        for (int i = count / 2; i < count; i++) {
            fromTailEntries.add(new AbstractEntry<>(TestUtils.generateKeyByIndex(i), String.valueOf(i)) {
            });
        }
        final List<Entry<String>> fromHeadEntries = new ArrayList<>();
        for (int i = 0; i < count / 2; i++) {
            fromHeadEntries.add(new AbstractEntry<>(TestUtils.generateKeyByIndex(i), String.valueOf(i)) {
            });
        }

        final Iterator<Entry<String>> orderedIterator = MergeIterator.of(
                List.of(
                        fromTailEntries.iterator(),
                        fromHeadEntries.iterator()),
                EntryComparator::compareStringEntries
        );

        Assert.assertEquals(count, fromTailEntries.size() + fromHeadEntries.size());
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(orderedIterator.hasNext());
            final Entry<String> next = orderedIterator.next();
            Assert.assertEquals(TestUtils.generateKeyByIndex(i), next.getKey());
            Assert.assertEquals(String.valueOf(i), next.getValue());
        }

        Assert.assertFalse(orderedIterator.hasNext());
    }
}

