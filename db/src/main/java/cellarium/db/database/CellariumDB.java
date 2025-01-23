package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.config.CellariumConfig;
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
import cellarium.db.exception.InvokeException;
import cellarium.db.exception.TimeoutException;
import cellarium.db.files.DiskComponent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public final class CellariumDB implements DataBase {
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final DiskComponent diskComponent;
    private final ExecutorService executorService;
    private final long flushSizeBytes;

    private final Lock flushLock = new ReentrantLock();

    public CellariumDB(CellariumConfig config) throws IOException {
        this.diskComponent = new DiskComponent(config.databasePath);
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.flushSizeBytes = config.flushSizeBytes;
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
        final Map<String, DataType> tableScheme = scheme.getColumnTypes();
        final Map<String, AValue<?>> memorySegmentValues = new HashMap<>(values.size());
        for (final Map.Entry<String, AValue<?>> column : values.entrySet()) {
            final String columnName = column.getKey();
            final DataType tableColumnType = tableScheme.get(columnName);
            if (tableColumnType == null) {
                throwNonexistentColumn(tableName, columnName);
            }

            final AValue<?> insertedColumn = column.getValue();
            checkTypesEquals(tableColumnType, insertedColumn.getDataType());

            sizeBytes += insertedColumn.getSizeBytesOnDisk();
            memorySegmentValues.put(columnName, insertedColumn);
        }

        if (sizeBytes + table.getMemTable().getSizeBytes() > flushSizeBytes) {
            scheduleFlush(tableName);
        }

        table.getMemTable().put(new MemorySegmentRow(MemorySegmentValueConverter.INSTANCE.convert(pk), memorySegmentValues, System.currentTimeMillis(), sizeBytes));
    }

    @Override
    public void deleteByPk(String tableName, AValue<?> pk) {
        final Table table = getTableWithChecks(tableName);
        checkTypesEquals(table.tableScheme.getPrimaryKey().getType(), pk.getDataType());
        final MemorySegmentValue key = MemorySegmentValueConverter.INSTANCE.convert(pk);

        table.getMemTable().put(new MemorySegmentRow(key, null, System.currentTimeMillis(), 0));
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

        final Iterator<Row<AValue<?>, AValue<?>>> memorySegmentRowIterator = table.getRange(fromMemorySegment, toMemorySegment);
        final TombstoneFilterIterator<MemorySegmentRow> tombstoneFilterIterator = new TombstoneFilterIterator<>(memorySegmentRowIterator);
        final DecodeIterator memotySegmentRowConverterIterator = new DecodeIterator(tombstoneFilterIterator);

        return new ColumnFilterIterator<>(memotySegmentRowConverterIterator, columns);
    }

    @Override
    public TableDescription describeTable(String tableName) {
        final TableScheme tableScheme = getTableWithChecks(tableName).tableScheme;

        return new TableDescription(tableName, new TableScheme(tableScheme.getPrimaryKey(), new HashMap<>(tableScheme.getColumnTypes()), new ArrayList<>(tableScheme.getScheme())));
    }

    @Override
    public List<TableDescription> describeTables() {
        return tables.values().stream().map(t -> new TableDescription(t.tableName, new TableScheme(t.tableScheme.getPrimaryKey(), new HashMap<>(t.tableScheme.getColumnTypes()), new ArrayList<>(t.tableScheme.getScheme())))).toList();
    }

    @Override
    public void createTable(String tableName, ColumnScheme pk, List<ColumnScheme> scheme) {
        if (pk == null || scheme == null) {
            throw new NullPointerException("Illegal argument");
        }

        try {
            synchronized (tables) {
                NameValidator.validateTableName(tableName);
                NameValidator.validateColumnName(pk.getName());

                final Map<String, DataType> columns = scheme.stream().collect(Collectors.toMap(ColumnScheme::getName, ColumnScheme::getType));

                if (scheme.size() != columns.size()) {
                    throw new IllegalStateException("Duplicated keys");
                }

                NameValidator.validateColumnNames(columns.keySet());

                final HashMap<String, DataType> tableScheme = new HashMap<>(columns);
                tableScheme.put(pk.getName(), pk.getType());

                final Table table = new Table(tableName, new TableScheme(pk, tableScheme, new ArrayList<>(scheme)), new MemTable<>(), new CopyOnWriteArrayList<>());
                diskComponent.createTable(tableName, table.tableScheme);
                tables.put(tableName, table);
            }
        } catch (IOException e) {
            throw new InvokeException("Table creation is failed", e);
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

            executorService.execute(() -> {
                try {
                    diskComponent.removeTableFromDisk(tableName);
                } catch (IOException e) {
                    // TODO: нужно фиксировать это дело
                }
            });
        }
    }


    @Override
    public void flush() throws IOException {
        for (Table table : tables.values()) {
            scheduleFlush(table.tableName);
        }
    }

    //TODO: тут гонка может быть над синхронизировать табличные модификации на специфичном локе
    private void scheduleFlush(String tableName) {
        final Table table = getTableWithChecks(tableName);
        flushLock.lock();
        try {
            if (table.hasFlushData()) {
                return;
            }
            table.flush();
        } finally {
            flushLock.unlock();
        }

        executorService.execute(() -> {
            if (!table.hasFlushData()) {
                throw new IllegalStateException("Scheduled flash without data");
            }

            final MemTable<MemorySegmentValue, MemorySegmentRow> flushTable = table.getFlushTable();
            try {
                table.addSSTable(diskComponent.flush(tableName, table.tableScheme, flushTable.get(null, null)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            table.clearFlushData();
        });
    }

    @Override
    public void close() throws IOException, TimeoutException {
        // TODO: Пока идет флаш, завершение,
        //  могут приходить модифицирующие изменения,
        //  нужно переставать принимать заявки

        flush();
        executorService.shutdown();

        try {
            final boolean terminated = executorService.awaitTermination(5, TimeUnit.MINUTES);
            if (!terminated) {
                executorService.shutdownNow();
                throw new TimeoutException("Await termination timeout, waited " + 5 + " minutes");
            }
        } catch (InterruptedException e) {
            // TODO: По-другому сделать
            Thread.currentThread().interrupted();
            throw new IOException(e);
        }
    }

    private static void checkColumnsInScheme(Table table, Set<String> columns) {
        if (columns == null) {
            return;
        }

        for (String column : columns) {
            if (table.tableScheme.getColumnType(column) == null) {
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
