package cellarium.db.converter.types;

import cellarium.db.MemorySegmentUtils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class BooleanMemorySegmentConverter implements MemorySegmentConverter<Boolean> {
    public static final BooleanMemorySegmentConverter INSTANCE = new BooleanMemorySegmentConverter();

    private BooleanMemorySegmentConverter() {}

    @Override
    public MemorySegment convert(Boolean value) {
        if (value == null) {
            return null;
        }

        return MemorySegmentUtils.ARENA_OF_AUTO.allocate(ValueLayout.JAVA_BYTE, (byte) (value ? 1 : 0));
    }

    @Override
    public Boolean convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return value.get(ValueLayout.JAVA_BYTE, 0) == 1;
    }
}
