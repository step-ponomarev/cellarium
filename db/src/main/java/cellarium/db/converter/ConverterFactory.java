package cellarium.db.converter;

import cellarium.db.converter.types.BooleanMemorySegmentConverter;
import cellarium.db.converter.types.IntegerConverterMemorySegmentConverter;
import cellarium.db.converter.types.LongMemorySegmentConverter;
import cellarium.db.converter.types.StringMemorySegmentConverter;
import cellarium.db.database.types.DataType;

import java.lang.foreign.MemorySegment;

import java.util.EnumMap;
import java.util.Map;

public final class ConverterFactory {
    private static Map<DataType, Converter<?, MemorySegment>> CONVERTERS = new EnumMap<>(DataType.class);

    static {
        CONVERTERS.put(DataType.LONG, LongMemorySegmentConverter.INSTANCE);
        CONVERTERS.put(DataType.INTEGER, IntegerConverterMemorySegmentConverter.INSTANCE);
        CONVERTERS.put(DataType.BOOLEAN, BooleanMemorySegmentConverter.INSTANCE);
        CONVERTERS.put(DataType.STRING, StringMemorySegmentConverter.INSTANCE);
    }

    private ConverterFactory() {}

    public static <T> Converter<T, MemorySegment> getConverter(DataType dataType) {
        final Converter<T, MemorySegment> converter = (Converter<T, MemorySegment>) CONVERTERS.get(dataType);
        if (converter == null) {
            throw new IllegalArgumentException("Unsupported converter for type: " + dataType);
        }

        return converter;
    }
}
