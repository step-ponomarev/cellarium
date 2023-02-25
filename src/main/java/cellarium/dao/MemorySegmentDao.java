package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
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

    private final long sizeLimit;
    private final ThreadSafeExecutor executor;

    private final DiskStore diskStore;

    private final Object scheduleFlushLock = new Object();

    private final Object flushCompactionLock = new Object();
    private final Runnable flushTask = new LockedTask(this::handlePreparedFlush, flushCompactionLock);

    private final MemoryStore memoryStore;

    public MemorySegmentDao(Path path, long limitBytes) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        this.sizeLimit = limitBytes;
        this.executor = new ThreadSafeExecutor(Executors.newFixedThreadPool(2));

        this.diskStore = new DiskStore(path);
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
        if (entrySize >= sizeLimit) {
            throw new IllegalStateException(
                    "Entry is too big, limit is " + sizeLimit + "bytes, entry size is: " + entry.getSizeBytes());
        }

        if (memoryStore.getSizeBytes() + entrySize >= sizeLimit && !memoryStore.hasFlushData()) {
            synchronized (scheduleFlushLock) {
                if (memoryStore.getSizeBytes() + entrySize >= sizeLimit && !memoryStore.hasFlushData()) {
                    memoryStore.prepareFlushData();
                    executor.execute(flushTask);
                }
            }
        }

        memoryStore.upsert(entry);
    }

    @Override
    public void flush() throws IOException {
        synchronized (flushCompactionLock) {
            if (memoryStore.hasFlushData()) {
                log.warn("Flush is running already!");
                return;
            }
            memoryStore.prepareFlushData();

            handleFlush();
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
                memoryStore.clearFlushData();
            }
        }
    }

    @Override
    public void close() throws IOException {
        executor.close(TimeUnit.MINUTES.toMillis(1));
        flush();
        diskStore.close();
    }

    private void handlePreparedFlush() {
        if (!memoryStore.hasFlushData()) {
            log.warn("Flushed already");
            return;
        }

        try {
            handleFlush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void handleFlush() throws IOException {
        final FlushData flushData = memoryStore.createFlushData();
        if (flushData == null) {
            throw new IllegalStateException("Flush is not prepared!");
        }

        try {
            if (!flushData.data.hasNext()) {
                log.warn("Flush task with empty data");
                return;
            }

            diskStore.flush(flushData);
        } finally {
            memoryStore.clearFlushData();
        }
    }
}
