package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import cellarium.entry.Entry;
import cellarium.iterators.ReadIterator;
import cellarium.DiskUtils;
import test.entry.EntryGeneratorList;
import test.entry.Utils;

public class CompactionDaoTest extends AConcurrentDaoTest {
    private static final long SIZE_BYTES = 1024 * 4; //4KB
    private static final String CURRENT_DIR = Paths.get(".").toAbsolutePath().normalize().toString();

    @Test
    public void testReadEachAfterCompaction() throws IOException {
        final int count = 100_000;

        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            entries.forEach(dao::upsert);

            dao.flush();
            dao.compact();

            for (int i = 0; i < count; i++) {
                final Entry<String> entry = entries.get(i);
                assertEquals(entry, dao.get(entry.getKey()));
            }
        }
    }

    @Test
    public void testOnlyOneDirAfterCompaction() throws IOException {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);
        final Path tmpDir = Files.createDirectory(Paths.get(CURRENT_DIR).resolve("test_dir_tmp"));

        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, Long.MAX_VALUE, false)) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
                dao.flush();
            }

            Assert.assertEquals(count, Files.list(tmpDir).count());
            dao.compact();

            Assert.assertEquals(1, Files.list(tmpDir).count());
        } finally {
            DiskUtils.removeDir(tmpDir);
        }
    }

    @Test
    public void testAllEntriesRemovedCompaction() throws IOException {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);
        final Path tmpDir = Files.createDirectory(Paths.get(CURRENT_DIR).resolve("test_dir_tmp"));

        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, Long.MAX_VALUE, false)) {
            entries.forEach(dao::upsert);
            dao.flush();

            for (Entry<String> entry : entries) {
                dao.upsert(createEntry(entry.getKey(), null));
                dao.flush();
            }

            dao.compact();
            Assert.assertEquals(0, Files.list(tmpDir).count());
        } finally {
            DiskUtils.removeDir(tmpDir);
        }
    }

    @Test
    public void testAvailableMemoryAfterHalfEntriesRemoved() throws IOException {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        final Path tmpDir = Files.createDirectory(Paths.get(CURRENT_DIR).resolve("test_dir_tmp"));
        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, Long.MAX_VALUE, false)) {
            entries.forEach(dao::upsert);
            dao.flush();

            final long beforeCompaction = DiskUtils.gerDirSizeBytes(tmpDir);
            for (int i = 0; i < count; i++) {
                if (i % 2 == 0) {
                    dao.upsert(createEntry(entries.get(i).getKey(), null));
                }
            }

            dao.flush();
            dao.compact();

            // Сжалось в двое, 1% погрешности
            Assert.assertTrue(beforeCompaction * 0.55 >= DiskUtils.gerDirSizeBytes(tmpDir));
        } finally {
            DiskUtils.removeDir(tmpDir);
        }
    }

    @Test
    public void testRemovedEntriesAfterCompaction() throws IOException {
        final int count = 2_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);
        final Path tmpDir = Files.createDirectory(Paths.get(CURRENT_DIR).resolve("test_dir_tmp"));

        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, Long.MAX_VALUE, false);) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }

            for (int i = 0; i < count; i++) {
                if (i % 2 == 0) {
                    dao.upsert(createEntry(entries.get(i).getKey(), null));
                    dao.flush();
                }
            }
            dao.compact();

            Assert.assertEquals(1, Files.list(tmpDir).count());
        } finally {
            DiskUtils.removeDir(tmpDir);
        }
    }

    @Test
    public void testReadAllAfterCompaction() throws IOException {
        final int count = 100_000;

        try (final Dao<String, Entry<String>> dao = createDao(SIZE_BYTES)) {
            final EntryGeneratorList entries = new EntryGeneratorList(count);
            entries.forEach(dao::upsert);

            dao.flush();
            dao.compact();

            final Iterator<Entry<String>> iterator = dao.get(null, null);
            for (int i = 0; i < count; i++) {
                final Entry<String> entry = entries.get(i);
                assertEquals(entry, iterator.next());
            }
        }
    }

    @Test
    public void testEmptyDaoCompact() throws IOException {
        final Path tmpDir = Files.createTempDirectory("test_dir_tmp");
        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, 0, false);) {
            dao.compact();

            Assert.assertTrue(DiskUtils.isDirEmpty(tmpDir));
        } finally {
            DiskUtils.removeDir(tmpDir);
        }
    }

    @Test(timeout = 15_000)
    public void testConcurrentWriteCompactReadEach() throws Exception {
        final int count = 1_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = createDao(Long.MAX_VALUE)) {
            runAsync(10, count, i -> {
                final Entry<String> addedEntry = entries.get(i);
                dao.upsert(addedEntry);
                dao.compact();

                assertEquals(addedEntry, dao.get(addedEntry.getKey()));
            }).await();
        }
    }

    @Test(timeout = 15_000, expected = ReadIterator.ReadException.class)
    public void testGetAllWhenCompaction() throws Exception {
        final int count = 100_000;
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = createDao(Long.MAX_VALUE)) {
            entries.forEach(dao::upsert);

            while (true) {
                final Iterator<Entry<String>> entryIterator = dao.get(null, null);
                dao.compact();

                while (entryIterator.hasNext()) {
                    entryIterator.next();
                }
            }
        }
    }

    @Test
    public void testAvailableMemoryIncreaseAfterCompaction() throws IOException {
        final int count = 10_000;
        final Path tmpDir = Files.createTempDirectory("test_dir_tmp");
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, Long.MAX_VALUE, false)) {
            long entriesSizeBytes = 0;
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
                entriesSizeBytes += Utils.getSizeBytesOf(entry);
            }

            Runtime.getRuntime().gc();
            final long freeMemoryBeforeFlush = Runtime.getRuntime().freeMemory();

            dao.compact();

            Runtime.getRuntime().gc();
            final long freeMemoryAfterFlush = Runtime.getRuntime().freeMemory();

            Assert.assertTrue(freeMemoryBeforeFlush - freeMemoryAfterFlush >= entriesSizeBytes);
        } finally {
            DiskUtils.removeDir(tmpDir);
        }
    }

    @Test
    public void testAutoCompaction() throws IOException {
        final int count = 10_000;
        final Path tmpDir = Files.createTempDirectory("test_dir_tmp");
        final EntryGeneratorList entries = new EntryGeneratorList(count);

        try (final Dao<String, Entry<String>> dao = createDao(tmpDir, 100, 4, true)) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }

            final long sstablesAmount = Files.list(tmpDir).count();
            Assert.assertTrue(sstablesAmount > 0);
            Assert.assertTrue(sstablesAmount <= 4);
        }
    }
}
