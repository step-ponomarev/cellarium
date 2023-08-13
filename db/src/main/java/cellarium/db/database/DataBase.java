package cellarium.db.database;

import java.util.Map;
import java.util.Set;

import cellarium.db.database.options.CreateTableOptions;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.database.types.TypedValue;

public interface DataBase {
    void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns, CreateTableOptions createTableOptions);

    void insert(String tableName, Map<String, TypedValue<?>> values);

    void select(String tableName, Set<String> columns, Condition condition);

    void update(String tableName, Map<String, TypedValue<?>> values, Condition condition);

    void delete(String tableName, Condition condition);

    void dropTable(String tableName);
}
