package cellarium.db.converter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.converter.value.MemorySegmentValueConverter;
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

        // version + timestamp
        long sizeBytes = Integer.BYTES + Long.BYTES;
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

        final MemorySegment pk = convertedColumns.get(0);
        MemorySegment.copy(pk, 0, row, currOffset, pk.byteSize());
        currOffset += pk.byteSize();

        row.set(ValueLayout.JAVA_INT_UNALIGNED, currOffset, value.getVersion());
        currOffset += Integer.BYTES;

        for (int i = 1; i < columnOrder.size(); i++) {
            final MemorySegment col = convertedColumns.get(i);
            MemorySegment.copy(col, 0, row, currOffset, col.byteSize());
            currOffset += col.byteSize();
        }

        row.set(ValueLayout.JAVA_LONG_UNALIGNED, currOffset, value.getTimestamp());

        return row;
    }

    @Override
    public MemorySegmentRow convertBack(MemorySegment value) {
        long currOffset = 0;

        final Map<String, AValue<?>> columns = new HashMap<>(columnOrder.size());
        final AValue<?> pk = SSTableValueConverter.INSTANCE.convertBack(value);
        currOffset += pk.getSizeBytesOnDisk();

        columns.put(columnOrder.get(0), pk);

        final int version = value.get(ValueLayout.JAVA_INT_UNALIGNED, currOffset);
        currOffset += Integer.BYTES;

        for (int i = 1; i < columnOrder.size(); i++) {
            final AValue<?> col = SSTableValueConverter.INSTANCE.convertBack(value.asSlice(currOffset));
            currOffset += col.getSizeBytesOnDisk();

            columns.put(columnOrder.get(i), col);
        }

        long timestamp = value.get(ValueLayout.JAVA_LONG_UNALIGNED, currOffset);

        return new MemorySegmentRow(MemorySegmentValueConverter.INSTANCE.convert(pk), columns, value.byteSize(), version, timestamp);
    }
}
