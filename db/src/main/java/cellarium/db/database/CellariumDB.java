package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.database.options.CreateTableOptions;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.database.types.TypedValue;
import cellarium.db.table.Table;
import cellarium.db.table.TableRow;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase {
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, MemTable<TypedValue, TableRow<TypedValue>>> memTables = new ConcurrentSkipListMap<>();

    @Override
    public void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns, CreateTableOptions createTableOptions) {
        validateTableName(tableName);
        validateColumnName(pk.getName());
        validateColumnNames(columns.keySet());

        final HashMap<String, DataType> tableScheme = new HashMap<>(columns);
        tableScheme.put(pk.getName(), pk.getType());

        synchronized (tables) {
            new MemTable<Object, TableRow<Object>>();
            tables.put(tableName, new Table(tableName, pk, tableScheme));
            memTables.put(tableName, new MemTable<>());
        }
    }

    @Override
    public void delete(String tableName, Condition condition) {
        validateTableName(tableName);
    }

    @Override
    public void dropTable(String tableName) {
        validateTableName(tableName);
        synchronized (tables) {
            tables.remove(tableName);
            memTables.remove(tableName);
        }
    }

    @Override
    public void insert(String tableName, Map<String, TypedValue> values) {
        validateTableName(tableName);
        final Table table = getTable(tableName);

        // Проверяем есть ли индексы у этой таблицы, которые мы можем использовать
        long sizeBytes = 0;
        final Map<String, DataType> tableScheme = table.getScheme();
        for (final Map.Entry<String, TypedValue> column : values.entrySet()) {
            final String columnName = column.getKey();
            final DataType tableColumnType = tableScheme.get(columnName);
            if (tableColumnType == null) {
                throw new IllegalStateException("The table \"" + tableName + "\" does not have a column named\" " + columnName + "\".");
            }

            final TypedValue insertedColumn = column.getValue();
            if (!tableColumnType.nativeType.equals(insertedColumn.getDataType().nativeType)) {
                throw new IllegalStateException("Expected column type: " + tableColumnType.name() + ", but got: " + insertedColumn.getDataType().name());
            }

            sizeBytes += insertedColumn.getSizeBytes();
        }

        final TypedValue pk = values.get(table.getPrimaryKey().getName());
        if (pk == null) {
            throw new IllegalStateException("PK is null");
        }

        final MemTable<TypedValue, TableRow<TypedValue>> memTable = memTables.get(tableName);
        memTable.put(
                new TableRow<>(pk, values, sizeBytes)
        );
    }

    //TODO: Обдумать как реализовать кандишены
    @Override
    public Iterator<TableRow<TypedValue>> select(String tableName, Set<String> columns, Condition condition) {
        validateTableName(tableName);
        final Table table = getTable(tableName);

        // Проверяем есть ли индексы для кондишина
        // если можем, то используем индексы

        return Collections.singleton(
                memTables.get(tableName).get(condition.equalsMap.get(table.getPrimaryKey().getName()))
        ).iterator();
    }

    @Override
    public void update(String tableName, Map<String, TypedValue> values, Condition condition) {
        // TODO Auto-generated method stub
        // Проверяем есть ли индексы у этой таблицы, которые мы можем использовать
    }

    private Table getTable(String tableName) {
        final Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalStateException("Table \"" + tableName + "\" does not exist.");
        }

        return table;
    }

    private static void validateColumnNames(Iterable<String> names) {
        if (names == null) {
            return;
        }

        for (String name : names) {
            validateColumnName(name);
        }
    }

    private static void validateColumnName(String name) {
        if (!Regex.COLUMN_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid column name: " + name);
        }
    }

    private static void validateTableName(String tableName) {
        if (!Regex.TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }
}
