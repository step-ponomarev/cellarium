package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.database.iterators.ColumnFilterIterator;
import cellarium.db.database.iterators.DecodeIterator;
import cellarium.db.database.iterators.TombstoneFilterIterator;
import cellarium.db.database.table.Table;
import cellarium.db.converter.value.MemorySegmentValueConverter;
import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableDescription;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.MemorySegmentValue;
import cellarium.db.database.validation.NameValidator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class CellariumDB implements DataBase {
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    public CellariumDB() {
        // TODO: config
    }

    @Override
    public void insert(String tableName, Map<String, AValue<?>> values) {
        final Table table = getTableWithChecks(tableName);
        final TableScheme scheme = table.tableScheme;

        final AValue<?> pk = values.get(scheme.getPrimaryKey().getName());
        if (pk == null) {
            throw new IllegalStateException("Primary key is not found");
        }

        long sizeBytes = 0;
        final Map<String, DataType> tableScheme = scheme.getScheme();
        final Map<String, AValue<?>> memorySegmentValues = new HashMap<>(values.size());
        for (final Map.Entry<String, AValue<?>> column : values.entrySet()) {
            final String columnName = column.getKey();
            final DataType tableColumnType = tableScheme.get(columnName);
            if (tableColumnType == null) {
                throwNonexistentColumn(tableName, columnName);
            }

            final AValue<?> insertedColumn = column.getValue();
            checkTypesEquals(tableColumnType, insertedColumn.getDataType());

            sizeBytes += insertedColumn.getSizeBytes();
            memorySegmentValues.put(columnName, insertedColumn);
        }

        table.memTable.put(new MemorySegmentRow(MemorySegmentValueConverter.INSTANCE.convert(pk), memorySegmentValues, sizeBytes));
    }

    @Override
    public void deleteByPk(String tableName, AValue<?> pk) {
        Table table = getTableWithChecks(tableName);
        checkTypesEquals(table.tableScheme.getPrimaryKey().getType(), pk.getDataType());
        final MemorySegmentValue key = MemorySegmentValueConverter.INSTANCE.convert(pk);

        table.memTable.put(new MemorySegmentRow(key, null, 0));
    }

    @Override
    public Row<AValue<?>, AValue<?>> getByPk(String tableName, AValue<?> pk, Set<String> columns) {
        final Iterator<Row<AValue<?>, AValue<?>>> range = getRange(tableName, pk, pk, columns);
        if (!range.hasNext()) {
            return null;
        }
        return range.next();
    }

    @Override
    public Iterator<Row<AValue<?>, AValue<?>>> getRange(String tableName, AValue<?> from, AValue<?> to, Set<String> columns) {
        Table table = getTableWithChecks(tableName);
        checkColumnsInScheme(table, columns);
        ColumnScheme primaryKey = table.tableScheme.getPrimaryKey();

        if (from != null) {
            checkTypesEquals(primaryKey.getType(), from.getDataType());
        }

        if (to != null) {
            checkTypesEquals(primaryKey.getType(), to.getDataType());
        }

        final MemorySegmentValue fromMemorySegment = MemorySegmentValueConverter.INSTANCE.convert(from);
        final MemorySegmentValue toMemorySegment = MemorySegmentValueConverter.INSTANCE.convert(to);

        final Iterator<MemorySegmentRow> memorySegmentRowIterator;
        if (fromMemorySegment != null && toMemorySegment != null && fromMemorySegment.compareTo(toMemorySegment) == 0) {
            MemorySegmentRow memorySegmentRow = table.memTable.get(fromMemorySegment);
            if (memorySegmentRow == null) {
                return Collections.emptyIterator();
            }

            memorySegmentRowIterator = List.of(memorySegmentRow).iterator();
        } else {
            memorySegmentRowIterator = table.memTable.get(
                    fromMemorySegment,
                    toMemorySegment
            );
        }

        final TombstoneFilterIterator<MemorySegmentRow> tombstoneFilterIterator = new TombstoneFilterIterator<>(memorySegmentRowIterator);
        final DecodeIterator memotySegmentRowConverterIterator = new DecodeIterator(tombstoneFilterIterator);

        return new ColumnFilterIterator<>(memotySegmentRowConverterIterator, columns);
    }

    @Override
    public TableDescription describeTable(String tableName) {
        final TableScheme tableScheme = getTableWithChecks(tableName).tableScheme;

        return new TableDescription(tableName, new TableScheme(tableScheme.getPrimaryKey(), new HashMap<>(tableScheme.getScheme())));
    }

    @Override
    public List<TableDescription> describeTables() {
        return tables.values().stream().map(t -> new TableDescription(t.tableName, new TableScheme(t.tableScheme.getPrimaryKey(), new HashMap<>(t.tableScheme.getScheme())))).toList();
    }

    @Override
    public void createTable(String tableName, ColumnScheme pk, Map<String, DataType> columns) {
        synchronized (tables) {
            NameValidator.validateTableName(tableName);
            NameValidator.validateColumnName(pk.getName());
            NameValidator.validateColumnNames(columns.keySet());

            final HashMap<String, DataType> tableScheme = new HashMap<>(columns);
            tableScheme.put(pk.getName(), pk.getType());

            tables.put(tableName, new Table(tableName, new TableScheme(pk, tableScheme), new MemTable<>()));
        }
    }

    @Override
    public void dropTable(String tableName) {
        synchronized (tables) {
            NameValidator.validateTableName(tableName);
            final Table removed = tables.remove(tableName);
            if (removed == null) {
                throw new IllegalStateException(STR."Table \"\{tableName}\" does not exist.");
            }
        }
    }

    private static void checkColumnsInScheme(Table table, Set<String> columns) {
        if (columns == null) {
            return;
        }

        for (String column : columns) {
            if (!table.tableScheme.getScheme().containsKey(column)) {
                throw new IllegalStateException("Table: " + table.tableName + " does not have column: " + column);
            }
        }
    }

    private static void throwNonexistentColumn(String tableName, String columnName) {
        throw new IllegalStateException(STR."The scheme \"\{tableName}\" does not have a column named \"\{columnName}\".");
    }

    private static void checkTypesEquals(DataType dataType, DataType otherDataType) {
        if (dataType.nativeType.equals(otherDataType.nativeType)) {
            return;
        }

        throw new IllegalStateException(STR."Expected column type: \{dataType.name()}, but got: \{otherDataType.name()}");
    }

    private Table getTableWithChecks(String tableName) {
        final Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalStateException(STR."Table \"\{tableName}\" does not exist.");
        }

        return table;
    }
}
