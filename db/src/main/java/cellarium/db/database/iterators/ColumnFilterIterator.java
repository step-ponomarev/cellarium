package cellarium.db.database.iterators;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class ColumnFilterIterator<T extends Row<?, ?>> implements Iterator<T> {
    private final Iterator<T> iter;
    private final Set<String> columns;

    public ColumnFilterIterator(Iterator<T> iter, Set<String> columns) {
        this.iter = iter;
        this.columns = columns == null || columns.isEmpty() ? null : columns;
    }

    @Override
    public boolean hasNext() {
        return this.iter.hasNext();
    }

    @Override
    public T next() {
        final T row = iter.next();
        if (columns == null) {
            return row;
        }

        final Map<String, AValue<?>> newValues = row.getValue()
                .entrySet()
                .stream()
                .filter(e -> columns.contains(e.getKey()))
                .collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                );

        return (T) new Row<>(
                row.getKey(),
                newValues
        );
    }
}
