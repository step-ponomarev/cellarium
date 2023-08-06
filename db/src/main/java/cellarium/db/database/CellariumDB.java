package cellarium.db.database;

import cellarium.db.MemTable;
import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.GetByIdQuery;
import cellarium.db.database.query.UpsertQuery;
import cellarium.db.database.query.validator.CreateTableQueryValidator;
import cellarium.db.database.query.validator.UpsertQueryValidator;
import cellarium.db.database.types.DataType;
import cellarium.db.entry.Entry;
import cellarium.db.table.TableRow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase {
    private final Map<String, Map<String, DataType>> tableSchemes = new HashMap<>();
    private final Map<String, MemTable<Long, TableRow<Long>>> memTables = new ConcurrentSkipListMap<>();

    @Override
    public void createTable(CreateTableQuery query) {
        CreateTableQueryValidator.INSTANCE.validate(query);

        final String tableName = query.getTableName();
        if (tableSchemes.containsKey(tableName)) {
            throw new IllegalStateException("Table with name " + tableName + " already exists");
        }

        final Map<String, DataType> columnsScheme = query.getColumnScheme();
        tableSchemes.put(tableName, columnsScheme);
        memTables.put(tableName, new MemTable<>());
    }


    @Override
    public void upsert(UpsertQuery query) {
        UpsertQueryValidator.INSTANCE.validate(query);

        final String tableName = query.getTableName();
        final Map<String, DataType> scheme = tableSchemes.get(tableName);
        if (scheme == null) {
            throw new IllegalStateException("Table with name " + tableName + " does not exist");
        }

        long sizeBytes = 0;
        for (Map.Entry<String, ?> v : query.getValues().entrySet()) {
            final String columnName = v.getKey();
            final DataType type = scheme.get(columnName);
            if (type == null) {
                throw new IllegalStateException("Table do not have column with name " + columnName);
            }

            if (!type.isTypeOf(v)) {
                throw new IllegalStateException("Invalid type of column " + columnName);
            }

            sizeBytes += DataType.sizeOf(v);
        }

        memTables.get(tableName).put(
                new TableRow<>(
                        query.getPk(),
                        query.getValues(),
                        sizeBytes
                )
        );
    }

    @Override
    public Entry<Long, ?> getById(GetByIdQuery query) {
        final String tableName = query.getTableName();

        final MemTable<Long, TableRow<Long>> memtable = memTables.get(tableName);
        if (memtable == null) {
            throw new IllegalStateException("Table with name " + tableName + " does not exist");
        }

        return memtable.get(
                query.getId()
        );
    }
}
