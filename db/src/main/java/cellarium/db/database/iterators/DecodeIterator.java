package cellarium.db.database.iterators;

import cellarium.db.converter.ColumnConverter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.MemorySegmentValue;
import jdk.incubator.foreign.MemorySegment;

import java.util.Iterator;

public final class DecodeIterator<I extends Iterator<MemorySegmentRow>> implements Iterator<Row<AValue<?>, AValue<?>>> {
    private final I iterator;

    public DecodeIterator(I iterator) {
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
                convertToValue(next.getKey()),
                next.getValue()
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
