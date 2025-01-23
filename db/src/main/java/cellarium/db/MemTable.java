package cellarium.db;

import cellarium.db.entry.Sizeable;
import cellarium.db.entry.WithKeyAndSize;
import cellarium.db.storage.Storage;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable<K extends Comparable<?>, E extends WithKeyAndSize<K>> implements Storage<K, E>, Sizeable {
    private final SortedMap<K, E> entries;
    private final AtomicLong sizeBytes;

    public MemTable() {
        this(null);
    }

    private MemTable(Comparator<K> comparator) {
        this.entries = new ConcurrentSkipListMap<>(comparator);
        this.sizeBytes = new AtomicLong(0);
    }

    @Override
    public Iterator<E> get(K from, K to) {
        return slice(entries, from, to);
    }

    @Override
    public E get(K key) {
        return entries.get(key);
    }

    @Override
    public void put(E entry) {
        entries.compute(entry.getKey(), (_, oldValue) -> {
            if (oldValue == null) {
                sizeBytes.addAndGet(entry.getSizeBytesOnDisk());
            } else {
                sizeBytes.addAndGet(entry.getSizeBytesOnDisk() - oldValue.getSizeBytesOnDisk());
            }

            return entry;
        });
    }

    public int getEntryCount() {
        return entries.size();
    }

    public AtomicLong getSizeBytesAtomic() {
        return sizeBytes;
    }

    public long getSizeBytesOnDisk() {
        return sizeBytes.get();
    }

    private Iterator<E> slice(SortedMap<K, E> store, K from, K to) {
        if (store == null || store.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return store.values().iterator();
        }

        if (from == null) {
            return store.headMap(to).values().iterator();
        }

        if (to == null) {
            return store.tailMap(from).values().iterator();
        }

        return store.subMap(from, to).values().iterator();
    }
}
