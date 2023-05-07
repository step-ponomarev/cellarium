package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import cellarium.dao.conf.TestDaoConfig;
import cellarium.dao.entry.AbstractEntry;
import cellarium.dao.entry.Entry;
import cellarium.dao.utils.DiskUtils;

public abstract class ADaoTest {
    public static final Path DEFAULT_DIR = Path.of("tmp");

    @Before
    public void init() throws IOException {
        if (Files.exists(DEFAULT_DIR)) {
            DiskUtils.removeDir(DEFAULT_DIR);
        }
        Files.createDirectory(DEFAULT_DIR);
    }

    @After
    public void cleanup() throws IOException {
        if (Files.exists(DEFAULT_DIR)) {
            DiskUtils.removeDir(DEFAULT_DIR);
        }
    }

    public final Entry<String> createEntry(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null!");
        }

        return new AbstractEntry<>(key, value) {
        };
    }

    protected final TestDaoConfig createConfig(long sizeBytes) {
        final TestDaoConfig testDaoConfig = new TestDaoConfig();
        testDaoConfig.path = DEFAULT_DIR.toString();
        testDaoConfig.memtableLimitBytes = sizeBytes;
        testDaoConfig.sstablesLimit = Integer.MAX_VALUE;


        return testDaoConfig;
    }

    protected final Entry<String> createEntryByIndex(int index) {
        return createEntry(String.valueOf(index), "value_" + index);
    }

    protected static void assertEquals(Entry<String> expected, Entry<String> actual) {
        if ((expected == null || actual == null) && !(actual == null && expected == null)) {
            throw new AssertionError("Expected: " + expected + " actual " + actual);
        }

        Assert.assertEquals(Objects.requireNonNull(expected).getKey(), Objects.requireNonNull(actual).getKey());
        Assert.assertEquals(expected.getValue(), actual.getValue());
    }

    protected static void assertContains(Iterator<Entry<String>> source, Entry<String> expected) {
        Entry<String> addedEntry = null;
        while (source.hasNext()) {
            addedEntry = source.next();
            if (expected.getKey().compareTo(addedEntry.getKey()) <= 0) {
                break;
            }
        }

        Assert.assertNotNull(addedEntry);
        if (expected.getKey().compareTo(addedEntry.getKey()) != 0 || expected.getValue().compareTo(addedEntry.getValue()) != 0) {
            Assert.fail("Data doesn't contain entry: " + expected.getKey() + " " + expected.getValue());
        }
    }

    protected static void assertNotContains(Iterator<Entry<String>> source, Entry<String> expected) {
        Entry<String> addedEntry = null;
        while (source.hasNext()) {
            addedEntry = source.next();
            if (expected.getKey().compareTo(addedEntry.getKey()) == 0) {
                Assert.fail("Contain entry: " + expected.getKey() + " " + expected.getValue());
            }
        }
    }
}
