package cellarium.db.database;

import cellarium.db.database.condition.Condition;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.Table;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public interface DataBase {
    void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns);

    void insert(String tableName, Map<String, AValue<?>> values);

    Iterator<Row<AValue<?>>> select(String tableName, Set<String> columns, Condition condition);

    void update(String tableName, Map<String, AValue<?>> values, Condition condition);

    void delete(String tableName, Condition condition);

    void dropTable(String tableName);

    Map<String, Table> describeTables();
}
