package cellarium.db.request;

import cellarium.db.TableColumn;

import java.util.Map;

public final class UpdateQuery<V> extends WhereQuery<V> {
    private final Map<String, TableColumn<V>> newValues;

    public UpdateQuery(String table, Map<String, TableColumn<V>> newValues, Map<String, TableColumn<V>> where) {
        super(table, where);
        this.newValues = newValues;
    }

    public Map<String, TableColumn<V>> getNewValues() {
        return newValues;
    }
}
