package cellarium.db.database.iterators;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

import cellarium.db.converter.SSTableRowConverter;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;

//TODO: В каждой строке в начале будет версия
public final class RowDecoderIterator implements Iterator<Row<AValue<?>, AValue<?>>> {
    private final Iterator<MemorySegment> source;
    private final SSTableRowConverter rowConverter;
    private MemorySegment curr = null;
    private long currOffset = 0;

    public RowDecoderIterator(Iterator<MemorySegment> source, SSTableRowConverter rowConverter) {
        this.source = source;
        this.rowConverter = rowConverter;
    }

    @Override
    public boolean hasNext() {
        if ((curr == null || currOffset > curr.byteSize()) && !source.hasNext()) {
            return false;
        }

        if (curr == null || currOffset > curr.byteSize()) {
            curr = source.next();
            currOffset = 0;
        }

        return true;
    }

    @Override
    public Row<AValue<?>, AValue<?>> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }

        final MemorySegmentRow memorySegmentRow = this.rowConverter.convertBack(curr);
        currOffset += memorySegmentRow.getSizeBytesOnDisk();

        return new Row<>(memorySegmentRow.getKey(), memorySegmentRow.getColumns());
    }
}
