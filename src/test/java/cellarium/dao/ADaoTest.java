package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import org.junit.Assert;
import cellarium.entry.Entry;
import cellarium.disk.DiskUtils;
import cellarium.entry.AbstractEntry;

public abstract class ADaoTest {
    private static final String WORKING_DIR = "dao_test";

    protected static Dao<String, Entry<String>> createDao(long bytesLimit) throws IOException {
        return createDao(bytesLimit, true);
    }

    protected static Dao<String, Entry<String>> createDao(long bytesLimit, boolean deleteDirAfterClose) throws IOException {
        final Path tempDirectory = Files.createTempDirectory(WORKING_DIR);

        return createDao(tempDirectory, bytesLimit, deleteDirAfterClose);
    }

    protected static Dao<String, Entry<String>> createDao(Path dir, long bytesLimit, boolean deleteDirAfterClose) throws IOException {
        if (dir != null && Files.notExists(dir)) {
            throw new IllegalStateException("Test dir is not exists");
        }

        final Path tempDirectory = dir == null ? Files.createTempDirectory(WORKING_DIR) : dir;

        return new TestDao(new MemorySegmentDao(tempDirectory, bytesLimit)) {
            @Override
            public void close() throws IOException {
                super.close();

                if (deleteDirAfterClose) {
                    DiskUtils.removeDir(tempDirectory);
                }
            }
        };
    }

    public Entry<String> createEntry(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null!");
        }

        return new AbstractEntry<>(key, value) { };
    }

    protected Entry<String> createEntryByIndex(int index) {
        return createEntry(String.valueOf(index), "value_" + index);
    }

    protected static void assertEquals(Entry<String> expected, Entry<String> actual) {
        if ((expected == null || actual == null) && !(actual == null && expected == null)) {
            throw new AssertionError("Expected: " + expected + " actual " + actual);
        }

        Assert.assertEquals(expected.getKey(), actual.getKey());
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
