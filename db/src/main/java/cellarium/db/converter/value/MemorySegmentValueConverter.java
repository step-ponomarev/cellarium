package cellarium.db.converter.value;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.comparator.ComparatorFactory;
import cellarium.db.converter.Converter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.MemorySegmentValue;

import java.lang.foreign.MemorySegment;

public final class MemorySegmentValueConverter implements Converter<AValue<?>, MemorySegmentValue> {
    public static final MemorySegmentValueConverter INSTANCE = new MemorySegmentValueConverter();

    @Override
    public MemorySegmentValue convert(AValue<?> value) {
        if (value == null) {
            return null;
        }

        final DataType dataType = value.getDataType();
        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);

        return new MemorySegmentValue(
                converter.convert(value.getValue()),
                dataType,
                value.getSizeBytesOnDisk(),
                ComparatorFactory.getComparator(dataType)
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
