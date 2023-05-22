package cellarium.db;

import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import cellarium.db.entry.Entry;
import entry.generator.EntryGeneratorList;

public class BasicConcurrentDaoTest extends AConcurrentDaoTest {
    private static final long SIZE_BYTES = 1024 * 4; //4KB

    @Test(timeout = 60_000)
    public void testConcurrentWrite() throws Exception {
        final int count = 10_000;

        try (final Dao<String, Entry<String>> dao = new TestDao(createConfig(SIZE_BYTES))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            runAsync(100, count, index -> dao.upsert(entries.get(index))).await();

            Iterator<Entry<String>> allEntries = dao.get(null, null);
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(allEntries.hasNext());
                assertEquals(entries.get(i), allEntries.next());
            }
        }
    }

    @Test(timeout = 60_000)
    public void testConcurrentWriteRead() throws Exception {
        final int count = 5_000;

        try (final Dao<String, Entry<String>> dao = new TestDao(createConfig(SIZE_BYTES))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            runAsync(100, count, index -> {
                final Entry<String> addedEntry = entries.get(index);
                dao.upsert(addedEntry);

                assertContains(dao.get(null, null), addedEntry);
            }).await();
        }
    }

    @Test(timeout = 60_000)
    public void testConcurrentRemoveReadEach() throws Exception {
        final int count = 10_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = new TestDao(createConfig(SIZE_BYTES))) {
            entries.forEach(dao::upsert);

            runAsync(100, count, index -> {
                final Entry<String> entryToRemove = entries.get(index);
                dao.upsert(createEntry(entryToRemove.getPk(), null));

                Assert.assertNull(dao.get(entryToRemove.getPk()));
            }).await();
        }
    }

    @Test(timeout = 60_000)
    public void testConcurrentRemoveReadAll() throws Exception {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = new TestDao(createConfig(SIZE_BYTES))) {
            entries.forEach(dao::upsert);

            runAsync(100, count, index -> {
                final Entry<String> entryToRemove = entries.get(index);
                dao.upsert(createEntry(entryToRemove.getPk(), null));

                assertNotContains(dao.get(null, null), entryToRemove);
            }).await();
        }
    }

    @Test(timeout = 60_000)
    public void testConcurrentRead() throws Exception {
        final int count = 2_500;

        try (final Dao<String, Entry<String>> dao = new TestDao(createConfig(SIZE_BYTES))) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            entries.forEach(dao::upsert);

            runAsync(100,
                    count,
                    index -> assertContains(dao.get(null, null), entries.get(index))
            ).await();

            Iterator<Entry<String>> allEntries = dao.get(null, null);
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(allEntries.hasNext());
                assertEquals(entries.get(i), allEntries.next());
            }
        }
    }
}
