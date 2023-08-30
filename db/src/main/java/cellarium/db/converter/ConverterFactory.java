package cellarium.db.converter;

import cellarium.db.database.types.DataType;
import jdk.incubator.foreign.MemorySegment;

import java.util.EnumMap;
import java.util.Map;

public final class ConverterFactory {
    private static Map<DataType, ColumnConverter<?, MemorySegment>> converters = new EnumMap<>(DataType.class);

    static {
        converters.put(DataType.LONG, LongMemorySegmentConverter.INSTANCE);
        converters.put(DataType.INTEGER, IntegerConverterMemorySegmentConverter.INSTANCE);
        converters.put(DataType.BOOLEAN, BooleanMemorySegmentConverter.INSTANCE);
        converters.put(DataType.STRING, StringMemorySegmentConverter.INSTANCE);
    }

    private ConverterFactory() {}

    public static <T> ColumnConverter<T, MemorySegment> getConverter(DataType dataType) {
        return (ColumnConverter<T, MemorySegment>) converters.get(dataType);
    }
}
