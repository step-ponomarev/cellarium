package cellarium.db.memtable;

import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import cellarium.db.entry.EntryComparator;
import cellarium.db.entry.MemorySegmentEntry;
import cellarium.db.store.Store;
import jdk.incubator.foreign.MemorySegment;

public class MemTable implements Store<MemorySegment, MemorySegmentEntry> {
    private final SortedMap<MemorySegment, MemorySegmentEntry> entries;
    private final AtomicLong sizeBytes;

    public MemTable() {
        this.entries = new ConcurrentSkipListMap<>(EntryComparator::compareMemorySegments);
        this.sizeBytes = new AtomicLong(0);
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) {
        return slice(entries, from, to);
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) {
        return entries.get(key);
    }

    @Override
    public void upsert(MemorySegmentEntry entry) {
        final long[] sizeDelta = new long[1];
        entries.compute(entry.getKey(), (k, oldValue) -> {
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

    private static Iterator<MemorySegmentEntry> slice(SortedMap<MemorySegment, MemorySegmentEntry> store,
                                                      MemorySegment from,
                                                      MemorySegment to) {
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
