package cellarium.db.database;

import cellarium.db.database.condition.Condition;
import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public interface DataBase {
    void createTable(String tableName, ColumnScheme pk, Map<String, DataType> columns);

    void insert(String tableName, Map<String, AValue<?>> values);

    Row<AValue<?>, AValue<?>> getByPk(String tableName, AValue<?> pk);

    void deleteByPk(String tableName, AValue<?> pk);

    void dropTable(String tableName);

    TableScheme describeTable(String tableName);

    Map<String, TableScheme> describeTables();

    Iterator<Row<AValue<?>, AValue<?>>> select(String tableName, Set<String> columns, Condition condition);

    void update(String tableName, Map<String, AValue<?>> values, Condition condition);

    void delete(String tableName, Condition condition);
}
