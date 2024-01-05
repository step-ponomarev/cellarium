package cellarium.db.converter.types;

import cellarium.db.MemorySegmentUtils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class IntegerMemorySegmentConverter implements MemorySegmentConverter<Integer> {
    public static final IntegerMemorySegmentConverter INSTANCE = new IntegerMemorySegmentConverter();

    private IntegerMemorySegmentConverter() {}

    @Override
    public MemorySegment convert(Integer value) {
        if (value == null) {
            return null;
        }

        return MemorySegmentUtils.ARENA_OF_AUTO.allocate(ValueLayout.JAVA_INT_UNALIGNED, value);
    }

    @Override
    public Integer convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return value.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
    }
}
