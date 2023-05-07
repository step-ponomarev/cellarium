package cellarium.dao.sstable;

import java.io.Closeable;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cellarium.dao.entry.MemorySegmentEntry;
import cellarium.dao.sstable.read.LockedEntryIterator;
import cellarium.dao.sstable.read.MappedEntryIterator;
import jdk.incubator.foreign.MemorySegment;

public final class SSTable implements Closeable {
    static final long TOMBSTONE_TAG = -1;
    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private final Index index;
    private final MemorySegment tableMemorySegment;

    /**
     * Guarantees state when read: alive / closed
     */
    private final ReadWriteLock readCloseLock = new ReentrantReadWriteLock();

    SSTable(MemorySegment indexMemorySegment, MemorySegment tableMemorySegment) {
        if (!indexMemorySegment.isReadOnly() || !tableMemorySegment.isReadOnly()) {
            throw new IllegalArgumentException("Mapped segments must be read only!");
        }

        this.index = new Index(indexMemorySegment, BYTE_ORDER);
        this.tableMemorySegment = tableMemorySegment;
    }

    @Override
    public void close() {
        readCloseLock.writeLock().lock();

        try {
            index.close();
            tableMemorySegment.scope().close();
        } finally {
            readCloseLock.writeLock().unlock();
        }
    }

    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return new LockedEntryIterator(
                    new MappedEntryIterator(
                            tableMemorySegment,
                            TOMBSTONE_TAG,
                            BYTE_ORDER
                    ),
                    readCloseLock.readLock()
            );
        }

        final int maxIndex = index.getMaxIndex();
        final int fromIndex = Math.abs(from == null ? 0 : index.findIndexOfKey(from, tableMemorySegment));

        if (fromIndex > maxIndex) {
            return Collections.emptyIterator();
        }

        final int toIndex = Math.abs(to == null ? index.getMaxIndex() + 1 : index.findIndexOfKey(to, tableMemorySegment));
        final long fromPosition = index.getEntryOffsetByIndex(fromIndex);
        final long toPosition = toIndex > maxIndex
                ? tableMemorySegment.byteSize()
                : index.getEntryOffsetByIndex(toIndex);

        return new LockedEntryIterator(
                new MappedEntryIterator(
                        tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition),
                        TOMBSTONE_TAG,
                        BYTE_ORDER
                ),
                readCloseLock.readLock()
        );
    }
}
