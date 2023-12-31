package cellarium.db.converter.value;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.converter.Converter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.BooleanValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.LongValue;
import cellarium.db.database.types.MemorySegmentValue;
import cellarium.db.database.types.StringValue;

import java.lang.foreign.MemorySegment;

public final class MemorySegmentValueConverter implements Converter<AValue<?>, MemorySegmentValue> {
    public static final MemorySegmentValueConverter INSTANCE = new MemorySegmentValueConverter();

    @Override
    public MemorySegmentValue convert(AValue<?> value) {
        if (value == null) {
            return null;
        }

        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(value.getDataType());
        return new MemorySegmentValue(
                converter.convert(value.getValue()),
                value.getDataType(),
                value.getSizeBytes()
        );
    }

    @Override
    public AValue<?> convertBack(MemorySegmentValue value) {
        return MemorySegmentUtils.toValue(
                value.getDataType(),
                value.getValue()
        );
    }
}
