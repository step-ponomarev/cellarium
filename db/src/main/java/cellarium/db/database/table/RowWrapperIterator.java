package cellarium.db.database.table;

import java.util.Iterator;
import java.util.NoSuchElementException;

import cellarium.db.database.types.AValue;

public final class RowWrapperIterator implements Iterator<Row<AValue<?>, AValue<?>>> {
    private final Iterator<MemorySegmentRow> source;

    public RowWrapperIterator(Iterator<MemorySegmentRow> source) {
        this.source = source;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public Row<AValue<?>, AValue<?>> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final MemorySegmentRow memorySegmentRow = source.next();
        return new Row<>(memorySegmentRow.getKey(), memorySegmentRow.getValue());
    }
}
