package cellarium.db.database.table;

import cellarium.db.MemTable;
import cellarium.db.database.types.MemorySegmentValue;

public final class Table {
    public final String tableName;
    public final TableScheme tableScheme;
    public final MemTable<MemorySegmentValue, MemorySegmentRow> memTable;

    public Table(String tableName, TableScheme tableScheme, MemTable<MemorySegmentValue, MemorySegmentRow> memTable) {
        this.tableName = tableName;
        this.tableScheme = tableScheme;
        this.memTable = memTable;
    }
}
