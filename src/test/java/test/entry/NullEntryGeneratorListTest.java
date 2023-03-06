package test.entry;

import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import cellarium.dao.entry.Entry;

public class NullEntryGeneratorListTest {
    @Test
    public void testNullListGeneration() {
        final int count = 10_000;
        final NullEntryGeneratorList entries = new NullEntryGeneratorList(count);
        Assert.assertEquals(count, entries.size());

        final Iterator<Entry<String>> iterator = entries.iterator();
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertTrue(iterator.next().getValue() == null);
        }

        Assert.assertFalse(iterator.hasNext());
    }
}
