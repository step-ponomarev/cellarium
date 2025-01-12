package cellarium.db.database.table;

import cellarium.db.database.types.AValue;
import cellarium.db.entry.Entry;

import java.util.Map;

public class Row<PK extends AValue<?>, V extends AValue<?>> implements Entry<PK, Map<String, V>> {
    private final PK pk;
    private final Map<String, V> columns;

    public Row(PK pk, Map<String, V> columns) {
        this.pk = pk;
        this.columns = columns;
    }

    @Override
    public Map<String, V> getColumns() {
        return columns;
    }

    @Override
    public PK getKey() {
        return pk;
    }
}
