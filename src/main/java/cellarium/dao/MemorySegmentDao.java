package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.entry.EntryComparator;
import cellarium.entry.MemorySegmentEntry;
import cellarium.iterators.MergeIterator;
import cellarium.iterators.ReadIterator;
import cellarium.iterators.TombstoneSkipIterator;
import cellarium.store.DiskStore;
import cellarium.store.FlushData;
import cellarium.store.MemoryStore;
import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentDao implements Dao<MemorySegment, MemorySegmentEntry> {
    private static final Logger log = LoggerFactory.getLogger(MemorySegmentDao.class);

    private final long memTableLimitBytes;
    private final int sstablesLimit;
    private final ExecutorService executor;

    private final Object scheduleFlushLock = new Object();
    private final Object flushCompactionLock = new Object();
    private final Runnable flushTask = new LockedTask(flushCompactionLock, this::handlePreparedFlush);

    private final MemoryStore memoryStore;
    private final DiskStore diskStore;

    public MemorySegmentDao(DaoConfig config) throws IOException {
        if (Files.notExists(config.path)) {
            throw new IllegalArgumentException("Path: " + config.path + " is not exist");
        }
        
        this.executor = Executors.newSingleThreadExecutor();

        this.memTableLimitBytes = config.memtableLimitBytes;
        this.sstablesLimit = config.sstablesLimit;

        this.diskStore = new DiskStore(config.path);
        this.memoryStore = new MemoryStore();
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        // Сначала с памяти потому что может произойти флаш и чтение с диска, когда
        // данных там еще не было.
        // А затем чтение из памяти, когда данные уже флашнули -> теряем данные
        final Iterator<MemorySegmentEntry> fromMemory = memoryStore.get(from, to);
        final Iterator<MemorySegmentEntry> fromDisk = diskStore.get(from, to);

        return new ReadIterator<>(
                new TombstoneSkipIterator<>(
                        MergeIterator.of(
                                List.of(fromDisk, fromMemory),
                                EntryComparator::compareMemorySegmentEntryKeys)
                )
        );
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        MemorySegmentEntry entry = memoryStore.get(key);
        if (entry != null) {
            return entry.getValue() == null ? null : entry;
        }

        entry = diskStore.get(key);
        if (entry != null) {
            return entry.getValue() == null ? null : entry;
        }

        return null;
    }

    @Override
    public void upsert(MemorySegmentEntry entry) {
        final long entrySize = entry.getSizeBytes();
        if (entrySize >= memTableLimitBytes) {
            throw new IllegalStateException(
                    "Entry is too big, limit is " + memTableLimitBytes + "bytes, entry size is: " + entry.getSizeBytes());
        }

        if (memoryStore.getSizeBytes() + entrySize >= memTableLimitBytes) {
            scheduleFlush(memoryStore.getSizeBytes() + entrySize);
        }

        memoryStore.upsert(entry);
    }

    private void scheduleFlush(long expectedSizeBytes) {
        synchronized (scheduleFlushLock) {
            if (memoryStore.hasFlushData() || expectedSizeBytes <= memTableLimitBytes) {
                return;
            }

            memoryStore.prepareFlushData();
            executor.execute(flushTask);
        }
    }

    @Override
    public void flush() throws IOException {
        synchronized (flushCompactionLock) {
            if (memoryStore.hasFlushData()) {
                log.warn("Flush is running already!");
                return;
            }

            memoryStore.prepareFlushData();
            try {
                doFlush();
            } finally {
                memoryStore.clearFlushData();
            }
        }
    }

    @Override
    public void compact() {
        synchronized (flushCompactionLock) {
            if (!memoryStore.hasFlushData()) {
                memoryStore.prepareFlushData();
            }

            try {
                diskStore.compact(memoryStore.createFlushData());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                //TODO: Не вышло закомпактиться - теряем данные??
                memoryStore.clearFlushData();
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (scheduleFlushLock) {
            executor.shutdown();
        }

        try {
            executor.awaitTermination(TimeUnit.MINUTES.toMillis(1), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        flush();
        diskStore.close();
    }

    private void handlePreparedFlush() {
        if (!memoryStore.hasFlushData()) {
            log.warn("Flushed already");
            return;
        }

        try {
            doFlush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            //TODO: Если не получилось - теряем данные?
            memoryStore.clearFlushData();
        }
    }

    private void doFlush() throws IOException {
        final FlushData flushData = memoryStore.createFlushData();
        if (flushData == null) {
            throw new IllegalStateException("Flush is not prepared!");
        }

        if (!flushData.data.hasNext()) {
            log.warn("Flush task with empty data");
            return;
        }

        if (sstablesLimit - 1 > diskStore.getSSTablesAmount()) {
            diskStore.flush(flushData);
        } else {
            log.info("Reached compaction limit: " + diskStore.getSSTablesAmount());
            diskStore.compact(flushData);
        }
    }
}
