package cellarium.db.table;

import cellarium.db.database.types.TypedValue;
import cellarium.db.entry.EntryWithSize;

import java.util.Map;

public final class TableRow<K> implements EntryWithSize<K, Map<String, ?>> {
    private final K key;
    private final Map<String, TypedValue> columns;
    private final long sizeBytes;

    public TableRow(K key, Map<String, TypedValue> columns, long sizeBytes) {
        this.key = key;
        this.columns = columns;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public Map<String, TypedValue> getValue() {
        return columns;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
