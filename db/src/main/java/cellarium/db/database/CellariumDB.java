package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.SelectQuery;
import cellarium.db.database.query.UpdateQuery;
import cellarium.db.database.query.UpsertQuery;
import cellarium.db.table.DataBaseColumnType;
import cellarium.db.table.TableRow;
import jdk.incubator.foreign.MemorySegment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase {
    private final Map<String, Map<String, DataBaseColumnType<?>>> tableSchemes = new HashMap<>();
    private final Map<String, MemTable<MemorySegment, TableRow<MemorySegment, ?>>> memTable = new ConcurrentSkipListMap<>();

    //TODO: Сделать конвертры
    private static final Map<Class<?>, MemorySegment> converters = new HashMap<>();

    @Override
    public void createTable(CreateTableQuery query) {
        final String tableName = query.getTable();
        final Map<String, DataBaseColumnType<?>> columnsScheme = query.getColumnScheme();

        checkCreateTableArgs(tableName, columnsScheme);

        if (tableSchemes.containsKey(tableName)) {
            throw new IllegalArgumentException("Table with name " + tableName + " already exists");
        }

        tableSchemes.put(tableName, columnsScheme);
        memTable.put(tableName, new MemTable<>());
    }


    @Override
    public void upsert(UpsertQuery query) {
        final String tableName = query.getTable();
        final Map<String, DataBaseColumnType<?>> scheme = tableSchemes.get(tableName);
        if (tableSchemes.containsKey(tableName)) {
            throw new IllegalArgumentException("Table with name " + tableName + " already exists");
        }
    }

    @Override
    public Iterator<TableRow<?, ?>> select(SelectQuery query) {
        return null;
    }

    @Override
    public void update(UpdateQuery query) {

    }

    private static void checkCreateTableArgs(String name, Map<String, DataBaseColumnType<?>> tableScheme) {
        if (name == null) {
            throw new NullPointerException("Name is null");
        }

        if (tableScheme == null) {
            throw new NullPointerException("Table scheme is null");
        }

        if (name.isBlank()) {
            throw new IllegalArgumentException("Table name is blank");
        }

        if (tableScheme.isEmpty()) {
            throw new IllegalArgumentException("Table scheme is empty");
        }
    }
}
