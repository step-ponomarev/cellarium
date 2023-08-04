package cellarium.db.request;

import cellarium.db.DataBaseColumnType;

import java.util.Map;

public class CreateTableQuery extends Query {
    private final Map<String, DataBaseColumnType<?>> columnScheme;

    public CreateTableQuery(String table, Map<String, DataBaseColumnType<?>> columnScheme) {
        super(table);
        this.columnScheme = columnScheme;
    }

    public Map<String, DataBaseColumnType<?>> getColumnScheme() {
        return columnScheme;
    }
}
