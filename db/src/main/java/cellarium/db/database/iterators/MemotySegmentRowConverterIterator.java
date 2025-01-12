package cellarium.db.database.iterators;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;

public final class MemotySegmentRowConverterIterator implements Iterator<Row<AValue<?>, AValue<?>>> {
    private final Iterator<MemorySegmentRow> source;
    private final String pkName;

    public MemotySegmentRowConverterIterator(Iterator<MemorySegmentRow> source, String pkName) {
        this.source = source;
        this.pkName = pkName;
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
        final Map<String, AValue<?>> value = memorySegmentRow.getValue();
        return new Row<>(value.get(pkName), value);
    }
}
