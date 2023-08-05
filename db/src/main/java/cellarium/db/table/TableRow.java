package cellarium.db.table;

import cellarium.db.entry.EntryWithSize;

import java.util.Map;

public final class TableRow<PK> implements EntryWithSize<PK, Map<String, ?>> {
    private final PK pk;
    private final Map<String, ?> columns;
    private final long sizeBytes;

    public TableRow(PK pk, Map<String, ?> columns, long sizeBytes) {
        this.pk = pk;
        this.columns = columns;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public PK getKey() {
        return pk;
    }

    @Override
    public Map<String, ?> getValue() {
        return columns;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
