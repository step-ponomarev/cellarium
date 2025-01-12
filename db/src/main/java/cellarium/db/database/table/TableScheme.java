package cellarium.db.database.table;

import cellarium.db.database.types.DataType;

import java.util.Map;

public final class TableScheme {
    private final ColumnScheme primaryKey;
    private final Map<String, DataType> scheme;

    public TableScheme(ColumnScheme primaryKey, Map<String, DataType> scheme) {
        this.primaryKey = primaryKey;
        this.scheme = scheme;
    }

    public ColumnScheme getPrimaryKey() {
        return primaryKey;
    }

    public Map<String, DataType> getScheme() {
        return scheme;
    }
}
