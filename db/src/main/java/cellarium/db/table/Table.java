package cellarium.db.table;

import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;

import java.util.Map;

public final class Table {
    private final String tableName;
    private final PrimaryKey primaryKey;
    private final Map<String, DataType> scheme;

    public Table(String tableName, PrimaryKey primaryKey, Map<String, DataType> scheme) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.scheme = scheme;
    }

    public String getTableName() {
        return tableName;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public Map<String, DataType> getScheme() {
        return scheme;
    }
}
