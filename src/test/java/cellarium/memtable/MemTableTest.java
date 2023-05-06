package cellarium.memtable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import cellarium.EntryConverter;
import cellarium.entry.MemorySegmentEntry;
import test.entry.EntryGeneratorList;

public class MemTableTest {
    public final ExecutorService executorService;

    public MemTableTest() {
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Test
    public void testConcurrencySizeCountingWorksCorrect() throws InterruptedException {
        final MemTable memTable = new MemTable();
        final AtomicLong totalSize = new AtomicLong(0);

        executorService.execute(
                () -> {
                    final EntryGeneratorList entries = new EntryGeneratorList(5000);
                    for (int j = 0; j < 5000; j++) {
                        final MemorySegmentEntry entry = EntryConverter.convert(entries.get(j));

                        memTable.upsert(entry);
                        totalSize.addAndGet(entry.getSizeBytes());

                        executorService.execute(() -> {
                            memTable.upsert(new MemorySegmentEntry(entry.getKey(), null, System.currentTimeMillis()));
                            totalSize.addAndGet(-(entry.getValue().byteSize()));
                        });
                    }
                }
        );

        executorService.awaitTermination(3, TimeUnit.SECONDS);
        Assert.assertEquals(totalSize.get(), memTable.getSizeBytes());
    }
}
