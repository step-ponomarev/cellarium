package cellarium.store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import cellarium.entry.EntryComparator;
import cellarium.entry.MemorySegmentEntry;
import cellarium.iterators.MergeIterator;
import cellarium.iterators.TombstoneSkipIterator;
import cellarium.sstable.SSTable;
import jdk.incubator.foreign.MemorySegment;

public class DiskStore implements Store<MemorySegment, MemorySegmentEntry>, Closeable {
    private final Path path;
    private final CopyOnWriteArrayList<SSTable> ssTables;

    public DiskStore(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        this.path = path;
        this.ssTables = new CopyOnWriteArrayList<>(SSTable.wakeUpSSTables(path));
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        return readFromDisk(from, to);
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        final Iterator<MemorySegmentEntry> data = readFromDisk(key, null);
        if (!data.hasNext()) {
            return null;
        }

        return data.next();
    }

    @Override
    public void close() {
        ssTables.forEach(SSTable::close);
    }

    public synchronized void flush(FlushData flushData) throws IOException {
        if (flushData == null || !flushData.data.hasNext()) {
            throw new IllegalStateException("Flushing data is empty!");
        }

        ssTables.add(
                SSTable.flushAndCreateSSTable(this.path, flushData.data, flushData.count, flushData.sizeBytes)
        );
    }

    public synchronized void compact(FlushData flushData) throws IOException {
        final List<Iterator<MemorySegmentEntry>> data = ssTables.stream()
                .map(table -> table.get(null, null))
                .collect(Collectors.toCollection(ArrayList::new));

        if (flushData != null) {
            data.add(flushData.data);
        }

        final FlushData compactedData = calculateFlushData(new TombstoneSkipIterator<>(
                MergeIterator.of(
                        data,
                        EntryComparator::compareMemorySegmentEntryKeys
                )
        ));

        final List<SSTable> ssTablesToRemove = new ArrayList<>(ssTables);
        final boolean hasCompactedData = compactedData.count != 0;
        if (hasCompactedData) {
            final SSTable ssTable = SSTable.flushAndCreateSSTable(
                    this.path,
                    compactedData.data,
                    compactedData.count,
                    compactedData.sizeBytes
            );

            ssTables.add(ssTable);
        }

        ssTables.removeAll(ssTablesToRemove);
        removeFromDisk(ssTablesToRemove);
    }

    public int getSSTablesAmount() {
        return ssTables.size();
    }

    private static void removeFromDisk(final List<SSTable> ssTables) throws IOException {
        if (ssTables == null || ssTables.isEmpty()) {
            return;
        }
        for (SSTable removed : ssTables) {
            removed.close();
            removed.removeSSTableFromDisk();
        }
    }

    private static FlushData calculateFlushData(Iterator<MemorySegmentEntry> data) {
        if (data == null || !data.hasNext()) {
            return new FlushData(
                    Collections.emptyIterator(),
                    0,
                    0
            );
        }

        final SortedMap<MemorySegment, MemorySegmentEntry> copy = new ConcurrentSkipListMap<>(EntryComparator::compareMemorySegments);
        int count = 0;
        long sizeBytes = 0;
        while (data.hasNext()) {
            final MemorySegmentEntry next = data.next();
            copy.put(next.getKey(), next);
            sizeBytes += next.getSizeBytes();
            count++;
        }

        return new FlushData(
                copy.values().iterator(),
                count,
                sizeBytes
        );
    }

    private Iterator<MemorySegmentEntry> readFromDisk(MemorySegment from, MemorySegment to) {
        return MergeIterator.of(
                ssTables.stream().map(ssTable -> ssTable.get(from, to)).toList(),
                EntryComparator::compareMemorySegmentEntryKeys
        );
    }
}
