package cellarium.db.database.table;

import cellarium.db.entry.WithKeyAndSize;
import cellarium.db.database.types.AValue;

import java.util.Map;

public final class TableRow<T, V extends AValue<T>> extends Row<V> implements WithKeyAndSize<V> {
    private final long sizeBytes;

    public TableRow(V pk, Map<String, V> columns, long sizeBytes) {
        super(pk, columns);
        this.sizeBytes = sizeBytes;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
