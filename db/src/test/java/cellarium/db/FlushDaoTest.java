package cellarium.db;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Test;
import cellarium.db.entry.Entry;
import entry.TestUtils;
import entry.generator.EntryGeneratorList;

public class FlushDaoTest extends AConcurrentDaoTest {
    private static final long UNLIMITED_MEMORY_SIZE = Long.MAX_VALUE;

    @Test
    public void testReadEachAfterFlush() throws IOException {
        final int count = 10_000;
        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            entries.forEach(dao::upsert);
            dao.flush();

            for (int i = 0; i < count; i++) {
                final Entry<String> entry = entries.get(i);

                assertEquals(entry, dao.get(entries.get(i).getKey()));
            }
        }
    }

    @Test(timeout = 10_000)
    public void testReadAllAfterFlush() throws IOException {
        final int count = 5_000;
        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            entries.forEach(dao::upsert);
            dao.flush();

            for (int i = 0; i < count; i++) {
                final Entry<String> entry = entries.get(i);

                assertContains(dao.get(null, null), entry);
            }
        }
    }

    @Test
    public void testFlushReplaceEntryFlush() throws IOException {
        final int count = 10_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            entries.forEach(dao::upsert);
            dao.flush();

            final int replaceIndex = count / 2;
            final Entry<String> entryWillBeReplaced = entries.get(replaceIndex);
            assertEquals(entries.get(replaceIndex), entryWillBeReplaced);

            final Entry<String> replaceEntry = createEntry(entryWillBeReplaced.getKey(), "REPLACED_VALUE");
            dao.upsert(replaceEntry);
            dao.flush();

            assertEquals(replaceEntry, dao.get(entryWillBeReplaced.getKey()));
        }
    }

    @Test
    public void testEveryFlushCreatesSSTable() throws IOException {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
                dao.flush();
            }

            Assert.assertEquals(count, Files.list(DEFAULT_DIR).count());
        }
    }

    @Test
    public void testGetEachRemovedDataAfterFlush() throws IOException {
        final int count = 10_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            entries.forEach(dao::upsert);
            dao.flush();

            for (int i = 0; i < count - 1; i += 2) {
                final Entry<String> entryToRemove = entries.get(i);
                dao.upsert(createEntry(entryToRemove.getKey(), null));
            }

            dao.flush();

            for (int i = 0; i < count; i++) {
                final Entry<String> expected = entries.get(i);
                if (i % 2 == 0) {
                    Assert.assertNull(dao.get(expected.getKey()));
                    continue;
                }

                assertEquals(expected, dao.get(expected.getKey()));
            }
        }
    }

    @Test
    public void testGetAllRemovedDataAfterFlush() throws IOException {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            entries.forEach(dao::upsert);
            dao.flush();

            for (int i = 0; i < count; i++) {
                final Entry<String> expected = entries.get(i);
                assertContains(dao.get(null, null), expected);
            }
        }
    }

    @Test
    public void testAvailableMemoryIncreaseAfterFlush() throws IOException {
        final int count = 1_000_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            long entriesSizeBytes = 0;
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
                entriesSizeBytes += TestUtils.getSizeBytesOf(entry);
            }

            Runtime.getRuntime().gc();
            final long freeMemoryBeforeFlush = Runtime.getRuntime().freeMemory();

            dao.flush();

            Runtime.getRuntime().gc();
            final long freeMemoryAfterFlush = Runtime.getRuntime().freeMemory();

            Assert.assertTrue(freeMemoryBeforeFlush - freeMemoryAfterFlush >= entriesSizeBytes);
        }
    }

    @Test
    public void testReadEachAfterReopen() throws Exception {
        final int count = 5_000;

        Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE));
        final EntryGeneratorList entries = new EntryGeneratorList(count);
        entries.forEach(dao::upsert);

        dao.close();
        dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE));

        for (Entry<String> entry : entries) {
            assertEquals(entry, dao.get(entry.getKey()));
        }

        dao.close();
    }

    @Test(timeout = 15_000)
    public void testWriteFlushReadConcurrent() throws Exception {
        final int count = 2_000;
        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);

            runAsync(100, count, i -> {
                final Entry<String> addedEntry = entries.get(i);

                dao.upsert(addedEntry);
                dao.flush();

                assertEquals(addedEntry, dao.get(addedEntry.getKey()));
            }).await();
        }
    }

    @Test(timeout = 60_000)
    public void testFlushCompactionReadEachConcurrent() throws Exception {
        final int count = 1_000;
        try (Dao<String, Entry<String>> dao = new TestDao(createConfig(UNLIMITED_MEMORY_SIZE))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);

            runAsync(100, count, i -> {
                final Entry<String> addedEntry = entries.get(i);

                dao.upsert(addedEntry);
                dao.flush();

                if (i % 2 == 0) {
                    dao.compact();
                }

                assertEquals(addedEntry, dao.get(addedEntry.getKey()));
            }).await();
        }
    }
}
