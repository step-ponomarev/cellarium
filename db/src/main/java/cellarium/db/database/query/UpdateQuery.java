package cellarium.db.database.query;

import cellarium.db.table.TableColumn;

import java.util.Map;

public final class UpdateQuery extends WhereQuery {
    private final Map<String, TableColumn<?>> newValues;

    public UpdateQuery(String table, Map<String, TableColumn<?>> newValues, Map<String, TableColumn<?>> where) {
        super(table, where);
        this.newValues = newValues;
    }

    public Map<String, TableColumn<?>> getNewValues() {
        return newValues;
    }
}
