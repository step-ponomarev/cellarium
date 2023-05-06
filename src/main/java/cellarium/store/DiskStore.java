package cellarium.store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import cellarium.entry.EntryComparator;
import cellarium.entry.MemorySegmentEntry;
import cellarium.iterators.MergeIterator;
import cellarium.iterators.TombstoneSkipIterator;
import cellarium.DiskUtils;
import cellarium.sstable.SSTableFactory;
import cellarium.sstable.SSTableWithMeta;
import jdk.incubator.foreign.MemorySegment;

public class DiskStore implements Store<MemorySegment, MemorySegmentEntry>, Closeable {
    private final Path path;
    private final CopyOnWriteArrayList<SSTableWithMeta> ssTables;

    public DiskStore(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        this.path = path;
        this.ssTables = new CopyOnWriteArrayList<>(SSTableFactory.wakeUpSSTables(path));
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        return readFromSSTables(from, to);
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        final Iterator<MemorySegmentEntry> data = readFromSSTables(key, null);
        if (!data.hasNext()) {
            return null;
        }

        return data.next();
    }

    @Override
    public void close() {
        ssTables.forEach(s -> s.ssTable.close());
    }

    public synchronized void flush(FlushData flushData) throws IOException {
        if (flushData == null || !flushData.data.hasNext()) {
            throw new IllegalStateException("Flushing data is empty!");
        }

        ssTables.add(
                SSTableFactory.flush(this.path, flushData.data, flushData.count, flushData.sizeBytes)
        );
    }

    public synchronized void compact(FlushData flushData) throws IOException {
        final List<Iterator<MemorySegmentEntry>> data = ssTables.stream()
                .map(table -> table.ssTable.get(null, null))
                .collect(Collectors.toCollection(ArrayList::new));

        if (flushData != null) {
            data.add(flushData.data);
        }

        final Iterator<MemorySegmentEntry> compactedData = new TombstoneSkipIterator<>(
                MergeIterator.of(
                        data,
                        EntryComparator::compareMemorySegmentEntryKeys
                )
        );

        final List<SSTableWithMeta> ssTablesToRemove = new ArrayList<>(ssTables);
        if (!compactedData.hasNext()) {
            ssTables.clear();
            removeSSTables(ssTablesToRemove);
            return;
        }

        ssTables.add(
                SSTableFactory.flush(this.path, compactedData)
        );

        ssTables.removeAll(ssTablesToRemove);
        removeSSTables(ssTablesToRemove);
    }

    public int getSSTablesAmount() {
        return ssTables.size();
    }

    private static void removeSSTables(final List<SSTableWithMeta> ssTables) throws IOException {
        if (ssTables == null || ssTables.isEmpty()) {
            return;
        }
        for (SSTableWithMeta removed : ssTables) {
            removed.ssTable.close();
            DiskUtils.removeDir(removed.sstableDir);
        }
    }

    private Iterator<MemorySegmentEntry> readFromSSTables(MemorySegment from, MemorySegment to) {
        return MergeIterator.of(
                ssTables.stream().map(ssTable -> ssTable.ssTable.get(from, to)).toList(),
                EntryComparator::compareMemorySegmentEntryKeys
        );
    }
}
