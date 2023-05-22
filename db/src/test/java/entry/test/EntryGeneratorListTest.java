package entry.test;

import org.junit.Assert;
import org.junit.Test;
import entry.generator.EntryGeneratorList;

public class EntryGeneratorListTest {
    @Test(expected = IndexOutOfBoundsException.class)
    public void testEmpty() {
        final EntryGeneratorList entries = new EntryGeneratorList(0);
        Assert.assertTrue(entries.isEmpty());

        entries.get(0);
    }

    @Test
    public void testBasicGeneration() {
        final int count = 10_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        Assert.assertEquals(count, entries.size());
        entries.forEach(e -> {
            Assert.assertTrue(e.getPk() != null);
            Assert.assertTrue(e.getValue() != null);
        });
    }

    @Test
    public void testCustomPrefixes() {
        final int count = 10_000;
        final String keyPrefix = "MY_KEY_";
        final String valuePrefix = "MY_VALUE_";
        final EntryGeneratorList entries = new EntryGeneratorList(count, keyPrefix, valuePrefix);

        Assert.assertEquals(count, entries.size());
        entries.forEach(e -> {
            Assert.assertTrue(e.getPk().startsWith(keyPrefix));
            Assert.assertTrue(e.getValue().startsWith(valuePrefix));
        });
    }
}
