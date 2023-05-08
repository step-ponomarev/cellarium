package cellarium.db.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cellarium.db.entry.EntryComparator;
import cellarium.db.entry.MemorySegmentEntry;
import cellarium.db.iterators.MergeIterator;
import jdk.incubator.foreign.MemorySegment;

public final class MemoryStore implements Store<MemorySegment, MemorySegmentEntry> {
    private final ReadWriteLock swapLock = new ReentrantReadWriteLock();
    private volatile CompositeMemTable compositeMemTable;

    public MemoryStore() {
        this.compositeMemTable = new CompositeMemTable();
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        swapLock.readLock().lock();
        try {
            final List<Iterator<MemorySegmentEntry>> data = new ArrayList<>();

            if (compositeMemTable.flushTable != null) {
                data.add(compositeMemTable.flushTable.get(from, to));
            }

            data.add(compositeMemTable.memTable.get(from, to));

            return MergeIterator.of(data, EntryComparator::compareMemorySegmentEntryKeys);
        } finally {
            swapLock.readLock().unlock();
        }
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        swapLock.readLock().lock();
        try {
            MemorySegmentEntry entry = compositeMemTable.memTable.get(key);
            if (entry != null) {
                return entry;
            }

            if (compositeMemTable.flushTable == null) {
                return null;
            }

            return compositeMemTable.flushTable.get(key);
        } finally {
            swapLock.readLock().unlock();
        }
    }

    @Override
    public void upsert(MemorySegmentEntry entry) {
        swapLock.readLock().lock();
        try {
            compositeMemTable.memTable.upsert(entry);
        } finally {
            swapLock.readLock().unlock();
        }
    }

    public long getSizeBytes() {
        swapLock.readLock().lock();
        try {
            return compositeMemTable.memTable.getSizeBytes();
        } finally {
            swapLock.readLock().unlock();
        }
    }

    public boolean flushIsPending() {
        swapLock.readLock().lock();
        try {
            return compositeMemTable.flushTable != null;
        } finally {
            swapLock.readLock().unlock();
        }
    }

    public int getCount() {
        swapLock.readLock().lock();
        try {
            return compositeMemTable.memTable.getEntryCount();
        } finally {
            swapLock.readLock().unlock();
        }
    }

    public FlushData createFlushData() {
        swapLock.readLock().lock();
        try {
            if (compositeMemTable.flushTable == null) {
                return null;
            }

            return new FlushData(
                    compositeMemTable.flushTable.get(null, null),
                    compositeMemTable.flushTable.getEntryCount(),
                    compositeMemTable.flushTable.getSizeBytes()
            );
        } finally {
            swapLock.readLock().unlock();
        }
    }

    public void prepareFlushData() {
        swapLock.writeLock().lock();
        try {
            compositeMemTable = CompositeMemTable.prepareToFlush(compositeMemTable);
        } finally {
            swapLock.writeLock().unlock();
        }
    }

    public void clearFlushData() {
        swapLock.writeLock().lock();
        try {
            compositeMemTable = CompositeMemTable.cleanFlushData(compositeMemTable);
        } finally {
            swapLock.writeLock().unlock();
        }
    }
}
