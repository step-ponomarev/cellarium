package cellarium.db.store;

import cellarium.db.memtable.MemTable;

final class CompositeMemTable {
    public final MemTable memTable;
    public final MemTable flushTable;

    public CompositeMemTable() {
        this.memTable = new MemTable();
        this.flushTable = null;
    }

    public CompositeMemTable(MemTable memTable, MemTable flushTable) {
        this.memTable = memTable;
        this.flushTable = flushTable;
    }

    public static CompositeMemTable prepareToFlush(CompositeMemTable store) {
        return new CompositeMemTable(
                new MemTable(),
                store.memTable
        );
    }

    public static CompositeMemTable cleanFlushData(CompositeMemTable store) {
        return new CompositeMemTable(
                store.memTable,
                null
        );
    }
}
