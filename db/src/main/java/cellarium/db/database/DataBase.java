package cellarium.db.database;

import cellarium.db.database.condition.Condition;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.table.TableRow;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public interface DataBase {
    void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns);

    void insert(String tableName, Map<String, AValue<?>> values);

    Iterator<TableRow<AValue<?>>> select(String tableName, Set<String> columns, Condition condition);

    void update(String tableName, Map<String, AValue<?>> values, Condition condition);

    void delete(String tableName, Condition condition);

    void dropTable(String tableName);
}
