package cellarium.db.database.iterators;

import cellarium.db.converter.value.MemorySegmentValueConverter;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.MemorySegmentValue;

import java.util.Iterator;

public final class DecodeIterator implements Iterator<Row<AValue<?>, AValue<?>>> {
    private final Iterator<MemorySegmentRow> iterator;

    public DecodeIterator(Iterator<MemorySegmentRow> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Row<AValue<?>, AValue<?>> next() {
        final Row<MemorySegmentValue, AValue<?>> next = this.iterator.next();
        return new Row<>(
                MemorySegmentValueConverter.INSTANCE.convertBack(next.getKey()),
                next.getValue()
        );
    }
}
