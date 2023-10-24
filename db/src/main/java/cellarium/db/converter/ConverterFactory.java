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
    private static Map<DataType, Converter<?, MemorySegment>> converters = new EnumMap<>(DataType.class);

    static {
        converters.put(DataType.LONG, LongMemorySegmentConverter.INSTANCE);
        converters.put(DataType.INTEGER, IntegerConverterMemorySegmentConverter.INSTANCE);
        converters.put(DataType.BOOLEAN, BooleanMemorySegmentConverter.INSTANCE);
        converters.put(DataType.STRING, StringMemorySegmentConverter.INSTANCE);
    }

    private ConverterFactory() {}

    public static <T> Converter<T, MemorySegment> getConverter(DataType dataType) {
        return (Converter<T, MemorySegment>) converters.get(dataType);
    }
}
