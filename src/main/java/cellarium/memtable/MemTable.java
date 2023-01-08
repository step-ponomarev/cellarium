package cellarium.memtable;

import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import cellarium.store.Store;
import jdk.incubator.foreign.MemorySegment;
import cellarium.entry.EntryComparator;
import cellarium.entry.MemorySegmentEntry;

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
        //TODO: Можно ли улучшить?
        entries.compute(entry.getKey(), (k, prev) -> {
            if (prev == null) {
                sizeBytes.addAndGet(entry.getSizeBytes());

                return entry;
            }

            final MemorySegment newEntryValue = entry.getValue();
            if (newEntryValue != null && EntryComparator.compareMemorySegments(prev.getValue(), newEntryValue) == 0) {
                return entry;
            }

            sizeBytes.addAndGet(entry.getSizeBytes() - prev.getSizeBytes());

            return entry;
        });
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
