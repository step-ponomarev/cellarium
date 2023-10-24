package cellarium.db.converter.types;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class IntegerConverterMemorySegmentConverter implements MemorySegmentConverter<Integer> {
    public static final IntegerConverterMemorySegmentConverter INSTANCE = new IntegerConverterMemorySegmentConverter();

    private IntegerConverterMemorySegmentConverter() {}

    @Override
    public MemorySegment convert(Integer value) {
        if (value == null) {
            return null;
        }

        return ARENA_OF_AUTO.allocate(ValueLayout.JAVA_INT, value);
    }

    @Override
    public Integer convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return value.get(ValueLayout.JAVA_INT, 0);
    }
}
