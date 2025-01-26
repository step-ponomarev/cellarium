package cellarium.db.database.table;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import cellarium.db.MemTable;
import cellarium.db.converter.SSTableRowConverter;
import cellarium.db.converter.sstable.SSTableKey;
import cellarium.db.database.iterators.DecodeIterator;
import cellarium.db.database.iterators.RowDecoderIterator;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.MemorySegmentValue;
import cellarium.db.sstable.SSTable;
import cellarium.db.storage.MergeIterator;

public final class Table {
    public final String tableName;
    public final TableScheme tableScheme;
    private volatile AtomicStorage atomicStorage;
    private final CopyOnWriteArrayList<SSTable> ssTables;

    private static final class AtomicStorage {
        private final MemTable<MemorySegmentValue, MemorySegmentRow> memTable;
        private final MemTable<MemorySegmentValue, MemorySegmentRow> flushTable;

        public AtomicStorage(MemTable<MemorySegmentValue, MemorySegmentRow> memTable, MemTable<MemorySegmentValue, MemorySegmentRow> flushTable) {
            this.memTable = memTable;
            this.flushTable = flushTable;
        }
    }

    public Table(String tableName,
                 TableScheme tableScheme,
                 MemTable<MemorySegmentValue, MemorySegmentRow> memTable,
                 CopyOnWriteArrayList<SSTable> ssTables
    ) {
        this.tableName = tableName;
        this.tableScheme = tableScheme;
        this.atomicStorage = new AtomicStorage(memTable, new MemTable<>());
        this.ssTables = ssTables;
    }

    public long getMemTableSizeBytes() {
        return this.atomicStorage.memTable.getSizeBytesOnDisk();
    }

    public void flush() {
        this.atomicStorage = new AtomicStorage(new MemTable<>(), this.atomicStorage.memTable);
    }

    public void clearFlushData() {
        this.atomicStorage = new AtomicStorage(this.atomicStorage.memTable, new MemTable<>());
    }

    public MemTable<MemorySegmentValue, MemorySegmentRow> getFlushTable() {
        return this.atomicStorage.flushTable;
    }

    public void addSSTable(SSTable ssTable) {
        this.ssTables.add(ssTable);
    }

    public boolean hasFlushData() {
        return this.atomicStorage.flushTable.getEntryCount() != 0;
    }

    public void put(MemorySegmentRow memorySegmentRow) {
        this.atomicStorage.memTable.put(memorySegmentRow);
    }


    public Iterator<Row<AValue<?>, AValue<?>>> getRange(MemorySegmentValue from, MemorySegmentValue to) {
        final Iterator<Row<AValue<?>, AValue<?>>> sstableValues = loadFromSSTables(from, to);
        final Iterator<Row<AValue<?>, AValue<?>>> flusDataIterator = new DecodeIterator(this.atomicStorage.flushTable.get(from, to));
        final Iterator<Row<AValue<?>, AValue<?>>> memDataIterator = new DecodeIterator(this.atomicStorage.memTable.get(from, to));

        return MergeIterator.of(
                List.of(sstableValues, flusDataIterator, memDataIterator),
                (l, r) -> ((Comparable) l.getKey()).compareTo(r.getKey()),
                (Row<AValue<?>, AValue<?>> source, Row<AValue<?>, AValue<?>> target) -> {
                    // tombstone merge not needed
                    if (target.getColumns() == null || source.getColumns() == null) {
                        return target;
                    }

                    final Row<AValue<?>, AValue<?>> merged = new Row<>(source.getKey(), new HashMap<>(source.getColumns()));
                    for (Map.Entry<String, AValue<?>> val : target.getColumns().entrySet()) {
                        merged.getColumns().put(val.getKey(), val.getValue());
                    }

                    return merged;
                }
        );
    }

    private Iterator<Row<AValue<?>, AValue<?>>> loadFromSSTables(MemorySegmentValue from, MemorySegmentValue to) {
        final DataType type = tableScheme.getPrimaryKey().getType();

        final SSTableKey sstableFromKey = from == null ? null : new SSTableKey(from.getValue(), type);
        final SSTableKey sstableToKey = to == null ? null : new SSTableKey(to.getValue(), type);

        //TODO: Merge row columns
        final Iterator<MemorySegment> sstableData = ssTables.stream()
                .map(ssTable -> ssTable.getDataRange(sstableFromKey, sstableToKey))
                .iterator();

        final Iterator<Row<AValue<?>, AValue<?>>> sstableValues = new RowDecoderIterator(
                sstableData,
                new SSTableRowConverter(tableScheme.getScheme().stream().map(ColumnScheme::getName).toList())
        );

        return sstableValues;
    }
}
