package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.converter.value.MemorySegmentValueConverter;
import cellarium.db.database.condition.Condition;
import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.MemorySegmentValue;
import cellarium.db.database.validation.NameValidator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase {
    private final Map<String, TableScheme> tables = new HashMap<>();
    private final Map<String, MemTable<MemorySegmentValue, MemorySegmentRow>> memTables = new ConcurrentSkipListMap<>();

    @Override
    public void createTable(String tableName, ColumnScheme pk, Map<String, DataType> columns) {
        NameValidator.validateTableName(tableName);
        NameValidator.validateColumnName(pk.getName());
        NameValidator.validateColumnNames(columns.keySet());

        final HashMap<String, DataType> tableScheme = new HashMap<>(columns);
        tableScheme.put(pk.getName(), pk.getType());

        tables.put(tableName, new TableScheme(tableName, pk, tableScheme));
        memTables.put(tableName, new MemTable<>());
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
        final TableScheme table = getTbaleWithChecks(tableName);

        final AValue<?> pk = values.get(table.getPrimaryKey().getName());
        if (pk == null) {
            throw new IllegalStateException("Primary key is null");
        }

        long sizeBytes = 0;
        final Map<String, DataType> tableScheme = table.getScheme();
        final Map<String, AValue<?>> memorySegmentValues = new HashMap<>(values.size());
        for (final Map.Entry<String, AValue<?>> column : values.entrySet()) {
            final String columnName = column.getKey();
            final DataType tableColumnType = tableScheme.get(columnName);
            if (tableColumnType == null) {
                throw new IllegalStateException("The table \"" + tableName
                        + "\" does not have a column named\" " + columnName + "\".");
            }

            final AValue<?> insertedColumn = column.getValue();
            if (!tableColumnType.nativeType.equals(insertedColumn.getDataType().nativeType)) {
                throw new IllegalStateException("Expected column type: " + tableColumnType.name()
                        + ", but got: " + insertedColumn.getDataType().name());
            }

            sizeBytes += insertedColumn.getSizeBytes();
            memorySegmentValues.put(columnName, insertedColumn);
        }

        memTables.get(tableName).put(
                new MemorySegmentRow(
                        MemorySegmentValueConverter.INSTANCE.convert(pk), memorySegmentValues, sizeBytes));
    }

    @Override
    public Row<AValue<?>, AValue<?>> getByPk(String tableName, AValue<?> pk) {
        NameValidator.validateTableName(tableName);
        final TableScheme table = getTbaleWithChecks(tableName);

        final MemorySegmentValue key = MemorySegmentValueConverter.INSTANCE.convert(pk);
        final MemorySegmentRow memorySegmentRow = memTables.get(table.getTableName()).get(key);

        return new Row<AValue<?>, AValue<?>>(pk, memorySegmentRow.getValue());
    }

    @Override
    public void deleteByPk(String tableName, AValue<?> pk) {
        final TableScheme table = getTbaleWithChecks(tableName);
        final MemorySegmentValue key = MemorySegmentValueConverter.INSTANCE.convert(pk);

        memTables.get(table.getTableName()).put(new MemorySegmentRow(key, null, 0));
    }

    @Override
    public TableScheme describeTable(String tableName) {
        return this.tables.get(tableName);
    }

    @Override
    public Map<String, TableScheme> describeTables() {
        return this.tables;
    }

    @Override
    public void update(String tableName, Map<String, AValue<?>> values, Condition condition) {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public Iterator<Row<AValue<?>, AValue<?>>> select(String tableName, Set<String> columns, Condition condition) {
        throw new UnsupportedOperationException("Comming soon");
    }

    @Override
    public void delete(String tableName, Condition condition) {
        throw new UnsupportedOperationException("Comming soon");
    }

    private TableScheme getTbaleWithChecks(String tableName) {
        final TableScheme table = tables.get(tableName);
        if (table == null) {
            throw new IllegalStateException("Table \"" + tableName + "\" does not exist.");
        }

        return table;
    }
}
