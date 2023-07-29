package cellarium.db.memtable;

import cellarium.db.entry.EntryWithSize;
import cellarium.db.storage.Storage;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable<K, E extends EntryWithSize<K, ?>> implements Storage<K, E> {
    private final SortedMap<K, E> entries;
    private final AtomicLong sizeBytes;

    public MemTable() {
        this(null);
    }

    public MemTable(Comparator<K> comparator) {
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
        final long[] sizeDelta = new long[1];
        entries.compute(entry.getPK(), (k, oldValue) -> {
            if (oldValue == null) {
                sizeDelta[0] = entry.getSizeBytes();
            } else {
                sizeDelta[0] = entry.getSizeBytes() - oldValue.getSizeBytes();
            }

            return entry;
        });

        if (sizeDelta[0] != 0) {
            sizeBytes.addAndGet(sizeDelta[0]);
        }
    }

    public int getEntryCount() {
        return entries.size();
    }

    public long getSizeBytes() {
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
