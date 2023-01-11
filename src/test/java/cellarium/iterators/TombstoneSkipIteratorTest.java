package cellarium.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import cellarium.entry.Entry;
import cellarium.entry.EntryComparator;
import test.entry.EntryGeneratorList;
import test.entry.NullEntryGeneratorList;

public class TombstoneSkipIteratorTest {
    @Test
    public void emptyIteratorTest() {
        Assert.assertFalse(
                new TombstoneSkipIterator<>(Collections.emptyIterator()).hasNext()
        );
    }

    @Test
    public void nullValuesIteratorTest() {
        final List<Entry<String>> nullIterator = new NullEntryGeneratorList(10_000);

        final Iterator<Entry<String>> iterator = nullIterator.iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertFalse(
                new TombstoneSkipIterator<>(iterator).hasNext()
        );
    }

    @Test
    public void notNullIteratorTest() {
        final int count = 10_000;

        final TombstoneSkipIterator<Entry<String>> iterator = new TombstoneSkipIterator<>(
                new EntryGeneratorList(count).iterator()
        );

        for (int i = 0; i < count; i++) {
            Assert.assertTrue(iterator.hasNext());
            iterator.next();
        }
    }

    @Test
    public void skipNullTest() {
        final int notNullCount = 10_000;
        final int nullCount = 2_000;

        final List<Iterator<Entry<String>>> iters = List.of(
                new EntryGeneratorList(notNullCount).iterator(),
                new NullEntryGeneratorList(nullCount, "NULL_").iterator()
        );

        final TombstoneSkipIterator<Entry<String>> iterator = new TombstoneSkipIterator<>(
                MergeIterator.of(
                        iters,
                        EntryComparator::compareStringEntries
                )
        );

        for (int i = 0; i < notNullCount; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertTrue(iterator.next().getValue() != null);
        }

        Assert.assertFalse(iterator.hasNext());
    }
}
