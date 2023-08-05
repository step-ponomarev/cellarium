package cellarium.db.database.query;

import cellarium.db.table.TableColumn;

import java.util.Map;
import java.util.Set;

public final class SelectQuery extends WhereQuery {
    private final Set<String> values;

    public SelectQuery(String table, Set<String> values, Map<String, TableColumn<?>> where) {
        super(table, where);
        this.values = values;
    }

    public Set<String> getValues() {
        return values;
    }
}
