package cellarium.dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import cellarium.entry.Entry;
import cellarium.iterators.ReadIterator;
import test.entry.EntryGeneratorList;

public class BasicDaoTest extends AbstractDaoTest {
    private static final Long SIZE_BYTES = 1024 * 4L; // 4KB

    @Test
    public void testEmptyDao() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            Iterator<Entry<String>> iterator = dao.get(null, null);
            Assert.assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testSingleEntry() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {

            final Entry<String> entry = createEntry("key", "value");
            dao.upsert(entry);

            final Entry<String> entryFromDao = dao.get(entry.getKey());
            Assert.assertEquals(entry.getKey(), entryFromDao.getKey());
            Assert.assertEquals(entry.getValue(), entryFromDao.getValue());
        }
    }

    @Test
    public void testSingleNullEntry() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final Entry<String> entry = createEntry("key", null);
            dao.upsert(entry);

            final Entry<String> entryFromDao = dao.get(entry.getKey());
            Assert.assertTrue(entryFromDao == null);
        }
    }

    @Test
    public void testGetEntriesByKey() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final EntryGeneratorList entries = new EntryGeneratorList(10);
            entries.forEach(dao::upsert);

            entries.forEach(e -> {
                final Entry<String> entry;
                try {
                    entry = dao.get(e.getKey());
                } catch (IOException ex) {
                    throw new IllegalStateException(ex);
                }

                Assert.assertEquals(e.getKey(), entry.getKey());
                Assert.assertEquals(e.getValue(), entry.getValue());
            });
        }
    }

    @Test
    public void testGetAllEntries() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final List<Entry<String>> entries = new EntryGeneratorList(30_000);
            entries.forEach(dao::upsert);

            final Iterator<Entry<String>> all = dao.get(null, null);
            for (int i = 0; i < entries.size(); i++) {
                Assert.assertTrue(all.hasNext());

                final Entry<String> entry = entries.get(i);
                final Entry<String> next = all.next();

                Assert.assertEquals(entry.getKey(), next.getKey());
                Assert.assertEquals(entry.getValue(), next.getValue());
            }

            Assert.assertFalse(all.hasNext());
        }
    }

    @Test
    public void testGetAllManyTimes() throws Exception {
        final int count = 10_000;

        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            entries.forEach(dao::upsert);

            final List<Iterator<Entry<String>>> iterators = IntStream.range(0, count).mapToObj(i -> {
                try {
                    return dao.get(null, null);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }).toList();

            Assert.assertEquals(count, iterators.size());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testTooBigEntry() throws Exception {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final byte[] randomBytes = new byte[(int) (SIZE_BYTES + 1)];
            ThreadLocalRandom.current().nextBytes(randomBytes);

            final Entry<String> biggerThanLimitEntry = createEntry("key", new String(randomBytes, StandardCharsets.UTF_8));
            dao.upsert(biggerThanLimitEntry);
        }
    }


    @Test
    public void testGetSingleValueFromMiddle() throws Exception {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {


            dao.upsert(createEntry("1", "a"));
            dao.upsert(createEntry("2", "b"));
            dao.upsert(createEntry("3", "c"));

            assertEquals(createEntry("2", "b"), dao.get("2"));
        }
    }

    @Test
    public void testGetRangeFromMiddle() throws Exception {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            dao.upsert(createEntry("1", "a"));
            dao.upsert(createEntry("2", "b"));
            dao.upsert(createEntry("3", "c"));

            final Iterator<Entry<String>> iterator = dao.get("2", "3");
            assertEquals(iterator.next(), createEntry("2", "b"));
            Assert.assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testGetFullRange() throws Exception {
        final int minKey = 0;
        final int maxKey = 9;

        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            for (int i = minKey; i < maxKey; i++) {
                dao.upsert(createEntryByIndex(i));
            }

            final Iterator<Entry<String>> iterator = dao.get("0", String.valueOf(maxKey));
            for (int i = minKey; i < maxKey; i++) {
                Assert.assertTrue(iterator.hasNext());
                final Entry<String> entry = iterator.next();

                assertEquals(createEntryByIndex(i), entry);
            }

            Assert.assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testRemoveAndRead() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final Entry<String> entry = createEntry("KEY", "VALUE");
            dao.upsert(entry);

            assertEquals(entry, dao.get(entry.getKey()));

            dao.upsert(createEntry(entry.getKey(), null));
            Assert.assertNull(dao.get(entry.getKey()));
        }
    }

    @Test
    public void testRemoveAndReadAll() throws IOException {
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final Entry<String> entry = createEntry("KEY", "VALUE");
            dao.upsert(entry);

            assertEquals(entry, dao.get(entry.getKey()));

            dao.upsert(createEntry(entry.getKey(), null));
            assertNotContains(dao.get(null, null), entry);
        }
    }

    @Test(expected = ReadIterator.ReadException.class)
    public void testReadAfterDaoClose() throws IOException {
        final int count = 100_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        Iterator<Entry<String>> dataFromDao;
        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            entries.forEach(dao::upsert);

            dataFromDao = dao.get(null, null);
        }

        dataFromDao.next();
    }

    @Test(timeout = 10000)
    public void testReadHugeData() throws Exception {
        final int count = 100_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            entries.forEach(dao::upsert);

            for (int i = 0; i < count; i++) {
                final Entry<String> expectedEnty = entries.get(i);
                assertEquals(expectedEnty, dao.get(expectedEnty.getKey()));
            }
        }
    }
}
