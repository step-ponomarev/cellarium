package cellarium.db.database.query;

import cellarium.db.database.types.DataType;

import java.util.Map;

public final class CreateTableQuery extends Query {
    private final Map<String, DataType> columnScheme;

    public CreateTableQuery(String table, Map<String, DataType> columnScheme) {
        super(table);
        this.columnScheme = columnScheme;
    }

    public Map<String, DataType> getColumnScheme() {
        return columnScheme;
    }
}
