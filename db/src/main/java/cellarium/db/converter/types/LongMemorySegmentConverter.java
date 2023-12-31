package cellarium.db.converter.types;

import cellarium.db.MemorySegmentUtils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class LongMemorySegmentConverter implements MemorySegmentConverter<Long> {
    public static final LongMemorySegmentConverter INSTANCE = new LongMemorySegmentConverter();

    private LongMemorySegmentConverter() {
    }

    @Override
    public MemorySegment convert(Long value) {
        if (value == null) {
            return null;
        }

        return MemorySegmentUtils.ARENA_OF_AUTO.allocate(ValueLayout.JAVA_LONG_UNALIGNED, value);
    }

    @Override
    public Long convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return value.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }
}
