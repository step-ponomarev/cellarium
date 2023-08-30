package cellarium.db.database.table;

import cellarium.db.database.types.AValue;
import cellarium.db.entry.Entry;

import java.util.Map;

public class Row<V extends AValue<?>> implements Entry<V, Map<String, V>> {
    private final V pk;
    private final Map<String, V> columns;

    public Row(V pk, Map<String, V> columns) {
        this.pk = pk;
        this.columns = columns;
    }

    @Override
    public Map<String, V> getValue() {
        return columns;
    }

    @Override
    public V getKey() {
        return pk;
    }
}
