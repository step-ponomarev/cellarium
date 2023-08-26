package cellarium.db.database;

import cellarium.db.database.options.CreateTableOptions;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.database.types.TypedValue;
import cellarium.db.table.TableRow;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public interface DataBase {
    void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns, CreateTableOptions createTableOptions);

    void insert(String tableName, Map<String, TypedValue> values);

    Iterator<TableRow<TypedValue>> select(String tableName, Set<String> columns, Condition condition);

    void update(String tableName, Map<String, TypedValue> values, Condition condition);

    void delete(String tableName, Condition condition);

    void dropTable(String tableName);
}
