package cellarium.db;

import cellarium.db.entry.Entry;
import cellarium.db.entry.WithKeyAndSize;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTableTest {
    private MemTable<String, EntryWithSize<String, String>> memTable;

    private interface EntryWithSize<K, V> extends Entry<K, V>, WithKeyAndSize<K> {
    }

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

        assertEquals(entry1, memTable.get(entry1.getKey()));
        assertEquals(entry2, memTable.get(entry2.getKey()));
    }

    @Test
    public void testUpdate() {
        final EntryWithSize<String, String> sourceEntry = createEntry("1", "value 1");

        memTable.put(sourceEntry);
        memTable.put(createEntry("2", "value 2"));

        final EntryWithSize<String, String> updatedEntry = createEntry(sourceEntry.getKey(), "new value 1");
        memTable.put(updatedEntry);

        final EntryWithSize<String, String> mayBeUpdatedEntry = memTable.get(sourceEntry.getKey());
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
    public void testConcurrencySizeBytes() {
        final int amount = 10000;
        final AtomicLong totalOffset = new AtomicLong(0);

        int planned = 0;
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        final AtomicInteger completed = new AtomicInteger(0);
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < amount; i++) {
                final int index = i;

                final EntryWithSize<String, String> entry = createEntry(String.valueOf(index), "value " + index);
                final CompletableFuture<Void> voidCompletableFuture = CompletableFuture.runAsync(() -> {
                    memTable.put(entry);
                    totalOffset.addAndGet(entry.getSizeBytes());
                    completed.incrementAndGet();
                }, executorService);
                planned++;
                futures.add(voidCompletableFuture);

                // удаляем каждый десятый
                if (index % 10 == 0) {
                    voidCompletableFuture.thenRunAsync(() -> {
                        final EntryWithSize<String, String> remove = createEntry(String.valueOf(index), null);
                        totalOffset.addAndGet(remove.getSizeBytes() - entry.getSizeBytes());
                        memTable.put(remove);
                        completed.incrementAndGet();
                    }, executorService);
                    planned++;
                }
            }

            final CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allFutures.orTimeout(5, TimeUnit.SECONDS).join();

            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertEquals(planned, completed.get());
            Assert.assertEquals(totalOffset.get(), memTable.getSizeBytes());
        } catch (InterruptedException e) {
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().isInterrupted();
            }

            throw new RuntimeException(e);
        }
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
                    memTable.put(createEntry(j + "_" + Thread.currentThread().hashCode(), "value " + j));
                }
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        Assert.assertEquals(threadCount * entryCount, memTable.getEntryCount());
    }

    private static boolean assertEquals(Entry<?, ?> e1, Entry<?, ?> e2) {
        return e1.getKey().equals(e2.getKey()) && Objects.equals(e1.getColumns(), e2.getColumns());
    }

    private static EntryWithSize<String, String> createEntry(String key, String value) {
        return new EntryWithSize<>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public String getColumns() {
                return value;
            }

            @Override
            public long getSizeBytes() {
                return StandardCharsets.UTF_8.encode(key).array().length + (value == null ? 0 : StandardCharsets.UTF_8.encode(value).array().length);
            }
        };
    }
}
