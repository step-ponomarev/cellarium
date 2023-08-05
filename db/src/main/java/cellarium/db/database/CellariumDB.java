package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.GetByIdQuery;
import cellarium.db.database.query.UpdateQuery;
import cellarium.db.database.query.UpsertQuery;
import cellarium.db.database.query.validator.CreateTableQueryValidator;
import cellarium.db.database.types.DataType;
import cellarium.db.entry.Entry;
import cellarium.db.table.TableRow;
import jdk.incubator.foreign.MemorySegment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase {
    private final Map<String, Map<String, DataType>> tableSchemes = new HashMap<>();
    private final Map<String, MemTable<Long, TableRow<Long>>> memTables = new ConcurrentSkipListMap<>();
    private static final Map<Class<?>, MemorySegment> converters = new HashMap<>();

    @Override
    public void createTable(CreateTableQuery query) {
        CreateTableQueryValidator.INSTANCE.validate(query);

        final String tableName = query.getTableName();
        if (tableSchemes.containsKey(tableName)) {
            throw new IllegalArgumentException("Table with name " + tableName + " already exists");
        }

        final Map<String, DataType> columnsScheme = query.getColumnScheme();
        tableSchemes.put(tableName, columnsScheme);
        memTables.put(tableName, new MemTable<>());
    }


    @Override
    public void upsert(UpsertQuery query) {
        final String tableName = query.getTableName();
        final Map<String, DataType> scheme = tableSchemes.get(tableName);
        if (scheme == null) {
            throw new IllegalArgumentException("Table with name " + tableName + " does not exist");
        }

        for (Map.Entry<String, ?> v : query.getValues().entrySet()) {
            final String columnName = v.getKey();
            final DataType type = scheme.get(columnName);
            if (type == null) {
                throw new IllegalArgumentException("Table do not have column with name " + columnName);
            }

            if (!type.isTypeOf(v)) {
                throw new IllegalArgumentException("Invalid type of column " + columnName);
            }
        }

        memTables.get(tableName).put(
                new TableRow<>(
                        query.getPk(),
                        query.getValues(),
                        //TODO: Считать вес
                        Long.MAX_VALUE
                )
        );
    }

    @Override
    public Entry<?, ?> getById(GetByIdQuery query) {
        final String tableName = query.getTableName();

        final MemTable<Long, TableRow<Long>> memtable = memTables.get(tableName);
        if (memtable == null) {
            throw new IllegalArgumentException("Table with name " + tableName + " does not exist");
        }

        return memtable.get(
                query.getId()
        );
    }

    @Override
    public void update(UpdateQuery query) {}
}
