package cellarium.db.converter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.types.AValue;

public class SSTableRowConverter implements Converter<MemorySegmentRow, MemorySegment> {
    private final List<String> columnOrder;

    public SSTableRowConverter(List<String> columnOrder) {
        this.columnOrder = Collections.unmodifiableList(columnOrder);
    }

    @Override
    public MemorySegment convert(MemorySegmentRow value) {
        final Map<String, AValue<?>> columns = value.getColumns();

        // timestamp on end
        long sizeBytes = Long.BYTES;
        final List<MemorySegment> convertedColumns = new ArrayList<>(columnOrder.size());
        for (String col : columnOrder) {
            // IF NULL, write nullable value
            final AValue<?> val = columns.get(col);

            final MemorySegment memorySegmentValue = SSTableValueConverter.INSTANCE.convert(val);
            convertedColumns.add(memorySegmentValue);
            sizeBytes += memorySegmentValue.byteSize();
        }

        long currOffset = 0;
        final MemorySegment row = MemorySegmentUtils.ARENA_OF_AUTO.allocate(sizeBytes);

        for (MemorySegment seg : convertedColumns) {
            MemorySegment.copy(seg, 0, row, currOffset, seg.byteSize());
            currOffset += seg.byteSize();
        }

        row.set(ValueLayout.JAVA_LONG_UNALIGNED, currOffset, value.getTimestamp());

        return row;
    }

    @Override
    public MemorySegmentRow convertBack(MemorySegment value) {

    }
}
