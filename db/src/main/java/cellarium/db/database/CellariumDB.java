package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.database.options.CreateTableOptions;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.database.types.TypedValue;
import cellarium.db.table.TableRow;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase {
    private final Map<String, Map<String, DataType>> tableSchemes = new HashMap<>();
    private final Map<String, MemTable<Long, TableRow<Long>>> memTables = new ConcurrentSkipListMap<>();

    @Override
    public void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns, CreateTableOptions createTableOptions) {
        if (!RegExr.TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name " + tableName);
        }


    }

    @Override
    public void delete(String tableName, Condition condition) {
        // TODO Auto-generated method stub

    }

    @Override
    public void dropTable(String tableName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void insert(String tableName, Map<String, TypedValue<?>> values) {
        // TODO Auto-generated method stub

    }

    @Override
    public void select(String tableName, Set<String> columns, Condition condition) {
        // TODO Auto-generated method stub

    }

    @Override
    public void update(String tableName, Map<String, TypedValue<?>> values, Condition condition) {
        // TODO Auto-generated method stub

    }

}
