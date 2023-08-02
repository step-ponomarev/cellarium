package cellarium.db;

import cellarium.db.entry.ColumnEntry;
import cellarium.db.storage.DiskStorage;
import jdk.incubator.foreign.MemorySegment;

import java.io.IOException;
import java.util.Iterator;

public final class CellariumDB implements DiskStorage<MemorySegment, ColumnEntry<MemorySegment, MemorySegment>> {
    private final MemTable<MemorySegment, ColumnEntry<MemorySegment, MemorySegment>> memTable = new MemTable<>(MemorySegmentComparator.INSTANCE);

    @Override
    public Iterator<ColumnEntry<MemorySegment, MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return memTable.get(from, to);
    }

    @Override
    public ColumnEntry<MemorySegment, MemorySegment> get(MemorySegment key) {
        return memTable.get(key);
    }

    @Override
    public void put(ColumnEntry<MemorySegment, MemorySegment> entry) {
        memTable.put(entry);
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
