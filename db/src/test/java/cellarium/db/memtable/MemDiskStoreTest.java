package cellarium.db.memtable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import cellarium.db.entry.MemorySegmentEntry;
import cellarium.db.utils.MemorySegmentUtils;
import entry.generator.EntryGeneratorList;

public class MemDiskStoreTest {

    @Test(timeout = 5_000)
    public void testConcurrencySizeBytesCountingWorksCorrect() throws InterruptedException, ExecutionException {
        final MemTable memTable = new MemTable();
        final AtomicLong totalSize = new AtomicLong(0);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final ExecutorService multiThreadExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        executorService.submit(
                () -> {
                    final EntryGeneratorList entries = new EntryGeneratorList(5000);
                    for (int j = 0; j < 5000; j++) {
                        final MemorySegmentEntry entry = MemorySegmentUtils.convert(entries.get(j));

                        memTable.upsert(entry);
                        totalSize.addAndGet(entry.getSizeBytes());

                        multiThreadExecutor.execute(() -> {
                            memTable.upsert(new MemorySegmentEntry(entry.getKey(), null, System.currentTimeMillis()));
                            totalSize.addAndGet(-(entry.getValue().byteSize()));
                        });
                    }

                    multiThreadExecutor.shutdown();
                    try {
                        multiThreadExecutor.awaitTermination(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
        ).get();

        Assert.assertEquals(totalSize.get(), memTable.getSizeBytes());
    }
}
