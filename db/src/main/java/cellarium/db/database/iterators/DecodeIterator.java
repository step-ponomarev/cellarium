package cellarium.db.database.iterators;

import cellarium.db.converter.ColumnConverter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.MemorySegmentValue;
import jdk.incubator.foreign.MemorySegment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class DecodeIterator implements Iterator<Row<? extends AValue<?>>> {
    private Iterator<? extends Row<MemorySegmentValue>> iterator;

    public DecodeIterator(Iterator<? extends Row<MemorySegmentValue>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Row<? extends AValue<?>> next() {
        final Row<? extends MemorySegmentValue> next = this.iterator.next();

        final Map<String, ? extends MemorySegmentValue> columns = next.getValue();

        final Map<String, AValue<?>> decodedColumns = new HashMap<>(columns.size());
        columns.entrySet().forEach(e -> decodedColumns.put(e.getKey(), convertToValue(e.getValue())));

        return new Row<>(
                next.getKey(),
                decodedColumns
        );
    }

    private static AValue<?> convertToValue(MemorySegmentValue encodedValue) {
        final DataType dataType = encodedValue.getDataType();
        final ColumnConverter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);

        return switch (dataType) {
            case INTEGER -> IntegerValue.of((Integer) converter.convertBack(encodedValue.getValue()));
            default -> throw new IllegalStateException("Unsupported data type");
        };
    }
}
