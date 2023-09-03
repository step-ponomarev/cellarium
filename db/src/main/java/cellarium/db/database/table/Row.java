package cellarium.db.database.table;

import cellarium.db.database.types.AValue;
import cellarium.db.entry.Entry;

import java.util.Map;

public class Row<K extends AValue<?>, V extends AValue<?>> implements Entry<K, Map<String, V>> {
    private final K pk;
    private final Map<String, V> columns;

    public Row(K pk, Map<String, V> columns) {
        this.pk = pk;
        this.columns = columns;
    }

    @Override
    public Map<String, V> getValue() {
        return columns;
    }

    @Override
    public K getKey() {
        return pk;
    }
}
