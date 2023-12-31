package cellarium.db;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import cellarium.db.converter.Converter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.BooleanValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.LongValue;
import cellarium.db.database.types.StringValue;

public class MemorySegmentUtils {
    public static final Arena ARENA_OF_AUTO = Arena.ofAuto();

    private MemorySegmentUtils() {
    }

    public static String memorySegmentToString(MemorySegment data) {
        if (data == null) {
            return null;
        }

        return data.getUtf8String(0);
    }

    public static MemorySegment stringToMemorySegment(String data) {
        if (data == null) {
            return null;
        }

        return ARENA_OF_AUTO.allocateUtf8String(data);
    }

    public static AValue<?> toValue(DataType dataType, MemorySegment value) {
        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);

        return switch (dataType) {
            case INTEGER -> IntegerValue.of((Integer) converter.convertBack(value));
            case LONG -> LongValue.of((Long) converter.convertBack(value));
            case BOOLEAN -> BooleanValue.of((Boolean) converter.convertBack(value));
            case STRING -> StringValue.of((String) converter.convertBack(value));
            default -> throw new IllegalStateException("Unsupported data type");
        };
    }

    public static MemorySegment readValue(MemorySegment segment, long offset) {
        final long valueSize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        return segment.asSlice(offset + Long.BYTES, valueSize);
    }

    /**
     * @param target
     * @param offset
     * @param value
     * @return offset after last entry
     */
    public static long writeValue(MemorySegment target, long offset, MemorySegment value) {
        target.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
        target.asSlice(offset + Long.BYTES, value.byteSize()).copyFrom(value);

        return offset + Long.BYTES + value.byteSize();
    }
}
