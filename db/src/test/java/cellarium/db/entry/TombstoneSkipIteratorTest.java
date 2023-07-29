package cellarium.db.entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


public class TombstoneSkipIteratorTest {

    @Test
    public void testSkipTombstones() {
        // Create a list of entries with some tombstones
        Entry<String, String> entry1 = new Entry<>("key1", "Value 1");
        Entry<String, String> entry2 = new Entry<>("key2", null); // Tombstone
        Entry<String, String> entry3 = new Entry<>("key3", "Value 3");
        Entry<String, String> entry4 = new Entry<>("key4", null); // Tombstone
        Entry<String, String> entry5 = new Entry<>("key5", "Value 5");

        List<Entry<String, String>> entryList = Arrays.asList(entry1, entry2, entry3, entry4, entry5);

        // Create the iterator with timeoutMs = 0 to skip tombstones
        Iterator<Entry<String, String>> iterator = entryList.iterator();
        TombstoneSkipIterator<Entry<String, String>> tombstoneSkipIterator = new TombstoneSkipIterator<>(iterator, 0);

        // Test the iterator
        List<String> resultList = new ArrayList<>();
        while (tombstoneSkipIterator.hasNext()) {
            resultList.add(tombstoneSkipIterator.next().getValue());
        }

        List<String> expectedList = Arrays.asList("Value 1", "Value 3", "Value 5");
        assertEquals(expectedList, resultList);
    }

    @Test
    public void testTimeout() {
        // Create a list of entries with tombstones
        Entry<String, String> entry1 = new Entry<>("key1", "Value 1");
        Entry<String, String> entry2 = new Entry<>("key2", null); // Tombstone
        Entry<String, String> entry3 = new Entry<>("key3", "Value 3");

        List<Entry<String, String>> entryList = Arrays.asList(entry1, entry2, entry3);

        // Create the iterator with timeoutMs = 1 to trigger a TimeoutException
        Iterator<Entry<String, String>> iterator = entryList.iterator();
        assertThrows(TombstoneSkipIterator.TimeoutException.class, () -> new TombstoneSkipIterator<>(iterator, 1));
    }

    @Test
    public void testEmptyIterator() {
        // Create an empty list
        List<Entry<String, String>> entryList = Collections.emptyList();

        // Create the iterator with timeoutMs = 0
        Iterator<Entry<String, String>> iterator = entryList.iterator();
        TombstoneSkipIterator<Entry<String, String>> tombstoneSkipIterator = new TombstoneSkipIterator<>(iterator, 0);

        // Test the iterator, should not throw an exception
        assertFalse(tombstoneSkipIterator.hasNext());
        assertThrows(NoSuchElementException.class, tombstoneSkipIterator::next);
    }

    // Test the TombstoneSkipIterator constructor with negative timeoutMs
    @Test
    public void testNegativeTimeout() {
        Iterator<Entry<String, String>> iterator = Collections.emptyIterator();
        assertThrows(IllegalArgumentException.class, () -> new TombstoneSkipIterator<>(iterator, -1));
    }
}

