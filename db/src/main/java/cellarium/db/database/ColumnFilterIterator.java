package cellarium.db.database;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class ColumnFilterIterator<T extends Row> implements Iterator<Row> {
    private final Iterator<T> iter;
    private final Set<String> columns;

    public ColumnFilterIterator(Iterator<T> iter, Set<String> columns) {
        this.iter = iter;
        this.columns = columns;
    }

    @Override
    public boolean hasNext() {
        return this.iter.hasNext();
    }

    @Override
    public Row next() {
        final T row = iter.next();

        final Map<String, AValue<?>> currentRowColumns = row.getValue();
        final Map<String, AValue<?>> newValues = new HashMap<>(columns.size());
        for (String column : columns) {
            newValues.put(column, currentRowColumns.get(column));
        }

        return new Row(
                row.getKey(),
                newValues
        );
    }
}
