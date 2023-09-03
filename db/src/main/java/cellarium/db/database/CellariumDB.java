package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.condition.Condition;
import cellarium.db.database.iterators.ColumnFilterIterator;
import cellarium.db.database.iterators.DecodeIterator;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.Table;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.MemorySegmentValue;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.database.validation.NameValidator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public final class CellariumDB implements DataBase {
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, MemTable<MemorySegmentValue, MemorySegmentRow>> memTables = new ConcurrentSkipListMap<>();

    @Override
    public void createTable(String tableName, PrimaryKey pk, Map<String, DataType> columns) {
        NameValidator.validateTableName(tableName);
        NameValidator.validateColumnName(pk.getName());
        NameValidator.validateColumnNames(columns.keySet());

        final HashMap<String, DataType> tableScheme = new HashMap<>(columns);
        tableScheme.put(pk.getName(), pk.getType());

        tables.put(tableName, new Table(tableName, pk, tableScheme));
        memTables.put(tableName, new MemTable<>());
    }

    @Override
    public void delete(String tableName, Condition condition) {
        NameValidator.validateTableName(tableName);
    }

    @Override
    public void dropTable(String tableName) {
        NameValidator.validateTableName(tableName);
        tables.remove(tableName);
        memTables.remove(tableName);
    }

    @Override
    public void insert(String tableName, Map<String, AValue<?>> values) {
        NameValidator.validateTableName(tableName);
        final Table table = getTable(tableName);

        long sizeBytes = 0;
        final Map<String, DataType> tableScheme = table.getScheme();
        final Map<String, AValue<?>> memorySegmentValues = new HashMap<>(values.size());
        for (final Map.Entry<String, AValue<?>> column : values.entrySet()) {
            final String columnName = column.getKey();
            final DataType tableColumnType = tableScheme.get(columnName);
            if (tableColumnType == null) {
                throw new IllegalStateException("The table \"" + tableName + "\" does not have a column named\" " + columnName + "\".");
            }

            final AValue<?> insertedColumn = column.getValue();
            if (!tableColumnType.nativeType.equals(insertedColumn.getDataType().nativeType)) {
                throw new IllegalStateException("Expected column type: " + tableColumnType.name() + ", but got: " + insertedColumn.getDataType().name());
            }

            sizeBytes += insertedColumn.getSizeBytes();
            memorySegmentValues.put(columnName, insertedColumn);
        }

        final AValue<?> pk = values.get(table.getPrimaryKey().getName());
        if (pk == null) {
            throw new IllegalStateException("Primary key is null");
        }

        memTables.get(tableName).put(new MemorySegmentRow(toMemorySegmentValue(pk), memorySegmentValues, sizeBytes));
    }

    private static MemorySegmentValue toMemorySegmentValue(AValue<?> value) {
        if (value == null) {
            return null;
        }

        return new MemorySegmentValue(
                ConverterFactory.getConverter(value.getDataType()).convert(value.getValue()),
                value.getDataType(),
                value.getSizeBytes()
        );
    }

    @Override
    public Iterator<Row<AValue<?>, AValue<?>>> select(String tableName, Set<String> columns, Condition condition) {
        NameValidator.validateTableName(tableName);
        final Table table = getTable(tableName);

        final Set<String> columnsInTable = table.getScheme().keySet();
        if (columns != null && !columnsInTable.containsAll(columns)) {
            final String badColumns = columns.stream().filter(c -> !columnsInTable.contains(c)).collect(Collectors.joining(", "));
            throw new IllegalStateException("Columns " + badColumns + " are not consist in table " + tableName);
        }

        Iterator<MemorySegmentRow> memorySegmentRowIterator = memTables.get(tableName).get(
                toMemorySegmentValue(condition.from),
                toMemorySegmentValue(condition.to)
        );

        return new DecodeIterator<>(
                new ColumnFilterIterator<>(
                        memorySegmentRowIterator,
                        columns
                )

        );
    }

    @Override
    public void update(String tableName, Map<String, AValue<?>> values, Condition condition) {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public Map<String, Table> describeTables() {
        return this.tables;
    }

    private Table getTable(String tableName) {
        final Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalStateException("Table \"" + tableName + "\" does not exist.");
        }

        return table;
    }
}
