package cellarium.db.database.table;

import cellarium.db.database.types.DataType;

import java.util.List;
import java.util.Map;

public final class TableScheme {
    private final int version;
    private final ColumnScheme primaryKey;
    private final Map<String, DataType> columnTypes;
    private final List<ColumnScheme> scheme;

    public TableScheme(ColumnScheme primaryKey, Map<String, DataType> columnTypes, List<ColumnScheme> scheme, int version) {
        this.primaryKey = primaryKey;
        this.columnTypes = columnTypes;
        this.scheme = scheme;
        this.version = version;
    }

    public ColumnScheme getPrimaryKey() {
        return primaryKey;
    }

    public DataType getColumnType(String columnName) {
        return columnTypes.get(columnName);
    }


    public List<ColumnScheme> getScheme() {
        return scheme;
    }

    public Map<String, DataType> getColumnTypes() {
        return columnTypes;
    }

    public int getVersion() {
        return version;
    }
}
