package cellarium.db;

import cellarium.db.request.CreateTableQuery;
import cellarium.db.request.SelectQuery;
import cellarium.db.request.UpdateQuery;
import jdk.incubator.foreign.MemorySegment;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class CellariumDB implements DataBase<MemorySegment> {
    private final Map<String, Map<String, DataBaseColumnType<?>>> tableSchemes = new HashMap<>();
    private final Map<String, MemTable<MemorySegment, TableRow<MemorySegment, MemorySegment>>> storage = new ConcurrentSkipListMap<>();

    //TODO: Сделать конвертры
    private static final Map<Class<?>, MemorySegment> converters = new HashMap<>();

    @Override
    public synchronized void createTable(CreateTableQuery query) {
        final String tableName = query.getTable();
        final Map<String, DataBaseColumnType<?>> columnsScheme = query.getColumnScheme();

        checkCreateTableArgs(tableName, columnsScheme);

        if (tableSchemes.containsKey(tableName)) {
            throw new IllegalArgumentException("Table with name " + tableName + " already exists");
        }

        tableSchemes.put(tableName, columnsScheme);
        storage.put(tableName, new MemTable<>());
    }

    @Override
    public Iterator<TableRow<MemorySegment, MemorySegment>> select(SelectQuery<MemorySegment> query) {
        final String tableName = query.getTable();

        if (!tableSchemes.containsKey(tableName)) {
            throw new IllegalArgumentException("Table with name " + tableName + " does not exist");
        }

        if (query.getWhere().size() != 1) {
            throw new UnsupportedOperationException("Select by PK only");
        }

        final Map.Entry<String, TableColumn<MemorySegment>> pkColumn = query.getWhere()
                .entrySet()
                .stream()
                .findFirst()
                .get();

        final Map<String, DataBaseColumnType<?>> scheme = tableSchemes.get(tableName);
        if (!scheme.containsKey(pkColumn.getKey())) {
            throw new IllegalArgumentException("The key " + pkColumn.getKey() + " is not included in table with name " + tableName);
        }

        final MemTable<MemorySegment, TableRow<MemorySegment, MemorySegment>> memTable = storage.get(tableName);

        TableRow<MemorySegment, MemorySegment> memorySegmentMemorySegmentTableRow = memTable.get(
                converters.get(
                        pkColumn.getValue().getType().getNativeType()
                )
        );

        return Collections.emptyIterator();
    }

    @Override
    public void update(UpdateQuery<MemorySegment> query) {

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
