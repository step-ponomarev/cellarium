package cellarium.db;

import cellarium.db.entry.EntryWithSize;

import java.util.Map;

public final class TableRow<PK, V> implements EntryWithSize<PK, Map<String, TableColumn<V>>> {
    private final PK pk;
    private final Map<String, TableColumn<V>> rowColumns;
    private final long sizeBytes;

    public TableRow(PK pk, Map<String, TableColumn<V>> rowColumns, long sizeBytes) {
        this.pk = pk;
        this.rowColumns = rowColumns;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public PK getPK() {
        return pk;
    }

    @Override
    public Map<String, TableColumn<V>> getValue() {
        return rowColumns;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
