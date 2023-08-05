package cellarium.db.database.query;

import cellarium.db.table.TableColumn;

import java.util.Map;

public final class UpsertQuery extends Query {
    private final TableColumn<?> pk;
    private final Map<String, TableColumn<?>> values;

    public UpsertQuery(String table, TableColumn<?> pk, Map<String, TableColumn<?>> values) {
        super(table);
        this.pk = pk;
        this.values = values;
    }

    public TableColumn<?> getPk() {
        return pk;
    }

    public Map<String, TableColumn<?>> getValues() {
        return values;
    }
}
