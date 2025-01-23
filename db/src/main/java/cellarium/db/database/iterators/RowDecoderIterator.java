package cellarium.db.database.iterators;

import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;

public final class RowDecoderIterator implements Iterator<Row<AValue<?>, AValue<?>>> {
    private final Iterator<MemorySegment> source;
    private MemorySegment curr = null;
    private long currOffset = 0;

    private final TableScheme tableScheme;

    public RowDecoderIterator(Iterator<MemorySegment> source, TableScheme tableScheme) {
        this.source = source;
        this.tableScheme = tableScheme;
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

        curr = curr.asSlice(currOffset);
        SSTableValueConverter.ValueWithSize val = SSTableValueConverter.INSTANCE.convertWithSize(curr);
        currOffset += val.getValueSizeOnDisk();

        //TODO: научиться работать с композитными PK
        ColumnScheme primaryKey = tableScheme.getPrimaryKey();
        //TODO: сделать один раз
        final List<String> scheme = tableScheme.getScheme()
                .stream()
                .filter(s -> !s.getName().equals(primaryKey.getName()))
                .map(ColumnScheme::getName)
                .toList();

        final Map<String, AValue<?>> readColumns = new HashMap<>(scheme.size());
        final AValue<?> pk = val.getValue();
        for (String column : scheme) {
            curr = curr.asSlice(currOffset);
            val = SSTableValueConverter.INSTANCE.convertWithSize(curr);
            currOffset += val.getValueSizeOnDisk();

            readColumns.put(
                    column,
                    val.getValue()
            );
        }

        return new Row<>(pk, readColumns);
    }
}
