package cellarium.db.database;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableDescription;
import cellarium.db.database.types.AValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataBase extends Closeable {
    void createTable(String tableName, ColumnScheme pk, List<ColumnScheme> scheme);

    void insert(String tableName, Map<String, AValue<?>> values);

    default Row<AValue<?>, AValue<?>> getByPk(String tableName, AValue<?> pk) {
        return getByPk(tableName, pk, null);
    }

    Row<AValue<?>, AValue<?>> getByPk(String tableName, AValue<?> pk, Set<String> columns);

    default Iterator<Row<AValue<?>, AValue<?>>> getRange(String tableName, AValue<?> from, AValue<?> to) {
        return getRange(tableName, from, to, null);
    }

    Iterator<Row<AValue<?>, AValue<?>>> getRange(String tableName, AValue<?> from, AValue<?> to, Set<String> columns);

    void deleteByPk(String tableName, AValue<?> pk);

    void dropTable(String tableName);

    TableDescription describeTable(String tableName);

    List<TableDescription> describeTables();

    void flush() throws IOException;

    void close() throws IOException;
}
