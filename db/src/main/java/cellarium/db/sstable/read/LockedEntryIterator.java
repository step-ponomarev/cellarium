package cellarium.db.sstable.read;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import cellarium.db.entry.MemorySegmentEntry;

public final class LockedEntryIterator implements Iterator<MemorySegmentEntry> {
    private final Iterator<MemorySegmentEntry> memorySegmentIterator;
    private final Lock lock;

    public LockedEntryIterator(Iterator<MemorySegmentEntry> memorySegmentIterator, Lock lock) {
        this.memorySegmentIterator = memorySegmentIterator;
        this.lock = lock;
    }

    @Override
    public boolean hasNext() {
        lock.lock();
        try {
            return memorySegmentIterator.hasNext();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public MemorySegmentEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        lock.lock();
        try {
            return memorySegmentIterator.next();
        } finally {
            lock.unlock();
        }
    }
}
