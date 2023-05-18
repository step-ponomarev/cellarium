package cellarium.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.db.entry.EntryComparator;
import cellarium.db.entry.MemorySegmentEntry;
import cellarium.db.iterators.MergeIterator;
import cellarium.db.iterators.ReadIterator;
import cellarium.db.iterators.TombstoneSkipIterator;
import cellarium.db.store.DiskStore;
import cellarium.db.store.FlushData;
import cellarium.db.store.MemoryStore;
import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentDao implements Dao<MemorySegment, MemorySegmentEntry> {
    private static final Logger log = LoggerFactory.getLogger(MemorySegmentDao.class);

    private final long memtableTotalSpaceBytes;
    private final int sstablesLimit;
    private final int timeoutMs;

    private final ExecutorService executor;

    private final Object scheduleFlushLock = new Object();
    private final Object flushCompactionLock = new Object();
    private final Runnable flushTask = new LockedTask(this::handlePreparedFlush, flushCompactionLock);

    private final MemoryStore memoryStore;
    private final DiskStore diskStore;

    public MemorySegmentDao(DaoConfig config) throws IOException {
        final Path path = Path.of(config.path);

        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " does not exist");
        }

        this.executor = Executors.newSingleThreadExecutor();
        this.memtableTotalSpaceBytes = config.memtableTotalSpaceBytes;
        this.sstablesLimit = config.sstablesLimit;
        this.timeoutMs = config.timeoutMs == null ? Integer.MAX_VALUE : config.timeoutMs;

        this.diskStore = new DiskStore(path);
        this.memoryStore = new MemoryStore();
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        final Iterator<MemorySegmentEntry> fromMemory = memoryStore.get(from, to);
        final Iterator<MemorySegmentEntry> fromDisk = diskStore.get(from, to);

        return new ReadIterator<>(
                new TombstoneSkipIterator<>(
                        MergeIterator.of(
                                // first old data (disk) then inmemory
                                List.of(fromDisk, fromMemory),
                                EntryComparator::compareMemorySegmentEntryKeys
                        ),
                        timeoutMs
                )
        );
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        MemorySegmentEntry entry = memoryStore.get(key);
        if (entry != null) {
            return entry;
        }

        return diskStore.get(key);
    }

    @Override
    public void upsert(MemorySegmentEntry entry) {
        final long entrySize = entry.getSizeBytes();
        if (entrySize > memtableTotalSpaceBytes) {
            throw new IllegalStateException(
                    "Entry is too big, limit is " + memtableTotalSpaceBytes + "bytes, entry size is: " + entry.getSizeBytes());
        }

        if (memoryStore.getSizeBytes() + entrySize > memtableTotalSpaceBytes) {
            scheduleFlush(memoryStore.getSizeBytes() + entrySize);
        }

        memoryStore.upsert(entry);
    }

    private void scheduleFlush(long expectedSize) {
        synchronized (scheduleFlushLock) {
            if (memoryStore.flushIsPending() || expectedSize <= memtableTotalSpaceBytes) {
                return;
            }

            memoryStore.prepareFlushData();
            executor.execute(flushTask);
        }
    }

    @Override
    public void flush() throws IOException {
        synchronized (flushCompactionLock) {
            if (memoryStore.flushIsPending()) {
                log.warn("Flush is running already!");
                return;
            }

            memoryStore.prepareFlushData();
            doFlush();
            memoryStore.clearFlushData();
        }
    }

    @Override
    public void compact() {
        synchronized (flushCompactionLock) {
            if (!memoryStore.flushIsPending()) {
                memoryStore.prepareFlushData();
            }

            try {
                diskStore.compact(
                        memoryStore.createFlushData()
                );
                memoryStore.clearFlushData();
            } catch (IOException e) {
                throw new IllegalStateException(e);
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
        } finally {
            flush();
            diskStore.close();
        }
    }

    private void handlePreparedFlush() {
        if (!memoryStore.flushIsPending()) {
            log.warn("Flushed already");
            return;
        }

        try {
            doFlush();
            memoryStore.clearFlushData();
        } catch (IOException e) {
            log.error("Flush is failed", e);
            throw new IllegalStateException(e);
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
            log.info("SStables limit is reached: " + sstablesLimit);
            diskStore.compact(flushData);
        }
    }
}
