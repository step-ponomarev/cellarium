package cellarium.db.entry;

import cellarium.db.Column;

public final class ColumnEntry<PK, V> implements EntryWithSize<PK, Column<?, V>[]> {
    private final PK pk;
    private final Column<?, V>[] columns;
    private final long sizeBytes;

    public ColumnEntry(PK pk, Column<?, V>[] columns, long sizeBytes) {
        this.pk = pk;
        this.columns = columns;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public PK getPK() {
        return pk;
    }

    @Override
    public Column<?, V>[] getValue() {
        return columns;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
