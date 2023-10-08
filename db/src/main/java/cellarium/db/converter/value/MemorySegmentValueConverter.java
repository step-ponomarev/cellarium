package cellarium.db.converter.value;

import cellarium.db.converter.Converter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.BooleanValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.LongValue;
import cellarium.db.database.types.MemorySegmentValue;
import cellarium.db.database.types.StringValue;
import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentValueConverter implements Converter<AValue<?>, MemorySegmentValue> {
    public static final MemorySegmentValueConverter INSTANCE = new MemorySegmentValueConverter();

    @Override
    public MemorySegmentValue convert(AValue<?> value) {
        if (value == null) {
            return null;
        }

        return new MemorySegmentValue(
                ConverterFactory.getConverter(value.getDataType()).convert(value.getValue()), value.getDataType(),
                value.getSizeBytes());
    }

    @Override
    public AValue<?> convertBack(MemorySegmentValue value) {
        final DataType dataType = value.getDataType();
        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);

        return switch (dataType) {
            case INTEGER -> IntegerValue.of((Integer) converter.convertBack(value.getValue()));
            case LONG -> LongValue.of((Long) converter.convertBack(value.getValue()));
            case BOOLEAN -> BooleanValue.of((Boolean) converter.convertBack(value.getValue()));
            case STRING -> StringValue.of((String) converter.convertBack(value.getValue()));
            default -> throw new IllegalStateException("Unsupported data type");
        };
    }
}
