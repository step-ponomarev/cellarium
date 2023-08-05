package cellarium.db.table;

import cellarium.db.entry.EntryWithSize;

import java.util.Map;

public final class TableRow<PK, V> implements EntryWithSize<PK, Map<String, TableColumn<V>>> {
    private final TableColumn<PK> pk;
    private final Map<String, TableColumn<V>> columns;
    private final long sizeBytes;

    public TableRow(TableColumn<PK> pk, Map<String, TableColumn<V>> columns, long sizeBytes) {
        this.pk = pk;
        this.columns = columns;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public PK getKey() {
        return pk.getValue();
    }

    @Override
    public Map<String, TableColumn<V>> getValue() {
        return columns;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }

    public TableColumn<PK> getPk() {
        return pk;
    }
}
