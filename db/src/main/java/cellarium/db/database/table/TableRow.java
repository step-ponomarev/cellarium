package cellarium.db.database.table;

import cellarium.db.database.types.AValue;
import cellarium.db.entry.WithKeyAndSize;

import java.util.Map;

public class TableRow<K extends AValue<?>> extends Row<K, AValue<?>> implements WithKeyAndSize<K> {
    private final long sizeBytes;

    public TableRow(K pk, Map<String, AValue<?>> columns, long sizeBytes) {
        super(pk, columns);
        this.sizeBytes = sizeBytes;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
