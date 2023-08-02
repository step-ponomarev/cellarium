package cellarium.db;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cellarium.db.entry.Entry;
import cellarium.db.entry.EntryWithSize;
import cellarium.db.memtable.MemTable;

public class MemTableTest {
    private MemTable<String, EntryWithSize<String, String>> memTable;

    @Before
    public void initMemTable() {
        memTable = new MemTable<>();
    }

    @Test
    public void testPutAndGet() {
        final EntryWithSize<String, String> entry1 = createEntry("1", "value 1");
        final EntryWithSize<String, String> entry2 = createEntry("2", "value 2");

        memTable.put(entry1);
        memTable.put(entry2);

        assertEquals(entry1, memTable.get(entry1.getPK()));
        assertEquals(entry2, memTable.get(entry2.getPK()));
    }

    @Test
    public void testUpdate() {
        final EntryWithSize<String, String> sourceEntry = createEntry("1", "value 1");

        memTable.put(sourceEntry);
        memTable.put(createEntry("2", "value 2"));

        final EntryWithSize<String, String> updatedEntry = createEntry(sourceEntry.getPK(), "new value 1");
        memTable.put(updatedEntry);

        final EntryWithSize<String, String> mayBeUpdatedEntry = memTable.get(sourceEntry.getPK());
        assertEquals(updatedEntry, mayBeUpdatedEntry);
    }

    @Test
    public void testSizeBytes() {
        final EntryWithSize<String, String> entry1 = createEntry("1", "value 1");
        final EntryWithSize<String, String> entry2 = createEntry("2", "value 2");

        memTable.put(entry1);
        memTable.put(entry2);

        Assert.assertEquals(entry1.getSizeBytes() + entry2.getSizeBytes(), memTable.getSizeBytes());
    }

    @Test
    public void testConcurrentPut() throws InterruptedException {
        final int threadCount = 100;
        final int entryCount = 1000;

        final CountDownLatch latch = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(entryCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < entryCount; j++) {
                    memTable.put(
                            createEntry(j + "_" + Thread.currentThread().hashCode(), "value " + j));
                }
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        Assert.assertEquals(threadCount * entryCount, memTable.getEntryCount());
    }

    private static boolean assertEquals(Entry<?, ?> e1, Entry<?, ?> e2) {
        return e1.getPK().equals(e2.getPK())
                && Objects.equals(e1.getValue(), e2.getValue());
    }

    private static EntryWithSize<String, String> createEntry(String pk, String value) {
        return new EntryWithSize<>() {
            @Override
            public String getPK() {
                return pk;
            }

            @Override
            public String getValue() {
                return value;
            }

            @Override
            public long getSizeBytes() {
                return StandardCharsets.UTF_8.encode(pk).array().length
                        + (value == null ? 0 : StandardCharsets.UTF_8.encode(value).array().length);
            }
        };
    }
}
