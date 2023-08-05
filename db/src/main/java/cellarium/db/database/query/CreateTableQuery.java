package cellarium.db.database.query;

import cellarium.db.table.DataBaseColumnType;

import java.util.Map;

public final class CreateTableQuery extends Query {
    private final Map<String, DataBaseColumnType<?>> columnScheme;

    public CreateTableQuery(String table, Map<String, DataBaseColumnType<?>> columnScheme) {
        super(table);
        this.columnScheme = columnScheme;
    }

    public Map<String, DataBaseColumnType<?>> getColumnScheme() {
        return columnScheme;
    }
}
