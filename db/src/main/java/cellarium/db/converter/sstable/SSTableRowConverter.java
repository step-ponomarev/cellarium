package cellarium.db.converter.sstable;

import java.lang.foreign.MemorySegment;
import java.util.Map;

import cellarium.db.converter.Converter;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;

//TODO: Как-то нужно универсально сделать?
// нужно знать схемму для convertBack?
public final class SSTableRowConverter implements Converter<MemorySegmentRow, MemorySegment> {
    private TableScheme scheme;

    @Override
    public MemorySegment convert(MemorySegmentRow value) {
        final Map<String, AValue<?>> columns = value.getColumns();

        return null;
    }

    @Override
    public MemorySegmentRow convertBack(MemorySegment value) {
        return null;
    }
}
