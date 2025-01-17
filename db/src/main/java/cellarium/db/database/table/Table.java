package cellarium.db.database.table;

import cellarium.db.MemTable;
import cellarium.db.database.types.MemorySegmentValue;

public final class Table {
    public final String tableName;
    public final TableScheme tableScheme;
    private AtomicStorage atomicStorage;

    private static final class AtomicStorage {
        private final MemTable<MemorySegmentValue, MemorySegmentRow> memTable;
        private final MemTable<MemorySegmentValue, MemorySegmentRow> flushTable;

        public AtomicStorage(MemTable<MemorySegmentValue, MemorySegmentRow> memTable, MemTable<MemorySegmentValue, MemorySegmentRow> flushTable) {
            this.memTable = memTable;
            this.flushTable = flushTable;
        }
    }

    public Table(String tableName, TableScheme tableScheme, MemTable<MemorySegmentValue, MemorySegmentRow> memTable) {
        this.tableName = tableName;
        this.tableScheme = tableScheme;
        this.atomicStorage = new AtomicStorage(memTable, null);
    }

    public void flush() {
        this.atomicStorage = new AtomicStorage(new MemTable<>(), this.atomicStorage.memTable);
    }

    public void clearFlushData() {
        this.atomicStorage = new AtomicStorage(this.atomicStorage.memTable, null);
    }

    public MemTable<MemorySegmentValue, MemorySegmentRow> getMemTable() {
        return this.atomicStorage.memTable;
    }

    public MemTable<MemorySegmentValue, MemorySegmentRow> getFlushTable() {
        return this.atomicStorage.flushTable;
    }

    public boolean hasFlushData() {
        return getFlushTable() != null;
    }
}
