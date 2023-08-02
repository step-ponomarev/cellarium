package cellarium.db;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.tombstone.Tombstone;
import cellarium.db.tombstone.TombstoneSkipIterator;

public class TombstoneSkipIteratorTest {

    @Test
    public void testNoTombstones() {
        final int expectedCount = 100;
        final Iterator<Tombstone> notTombstones = IntStream.range(0, expectedCount)
                .mapToObj(i -> createTombstone(false))
                .iterator();

        int count = 0;
        final TombstoneSkipIterator<Tombstone> tombstoneSkipIterator = new TombstoneSkipIterator<>(notTombstones);
        while (tombstoneSkipIterator.hasNext()) {
            tombstoneSkipIterator.next();
            count++;
        }

        Assert.assertEquals(expectedCount, count);
    }

    @Test
    public void testSkipTombstones() {
        final int expectedCount = 100;
        final Iterator<Tombstone> tombstones = IntStream.range(0, expectedCount)
                .mapToObj(i -> createTombstone((i & 1) == 0))
                .iterator();

        int count = 0;
        final TombstoneSkipIterator<Tombstone> tombstoneSkipIterator = new TombstoneSkipIterator<>(tombstones);
        while (tombstoneSkipIterator.hasNext()) {
            tombstoneSkipIterator.next();
            count++;
        }

        Assert.assertEquals(expectedCount / 2, count);
    }

    @Test(expected = NullPointerException.class)
    public void testTimeout() {
        new TombstoneSkipIterator<>(null);
    }

    private static Tombstone createTombstone(boolean tombstone) {
        return () -> tombstone;
    }
}
