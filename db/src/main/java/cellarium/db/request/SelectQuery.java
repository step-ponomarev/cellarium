package cellarium.db.request;

import cellarium.db.TableColumn;

import java.util.Map;
import java.util.Set;

public final class SelectQuery<V> extends WhereQuery<V> {
    private final Set<String> values;

    public SelectQuery(String table, Set<String> values, Map<String, TableColumn<V>> where) {
        super(table, where);
        this.values = values;
    }

    public Set<String> getValues() {
        return values;
    }
}
