package cellarium.db.converter.types;

import cellarium.db.MemorySegmentUtils;

import java.lang.foreign.MemorySegment;

public final class StringMemorySegmentConverter implements MemorySegmentConverter<String> {
    public static final StringMemorySegmentConverter INSTANCE = new StringMemorySegmentConverter();

    @Override
    public MemorySegment convert(String value) {
        if (value == null) {
            return null;
        }

        return MemorySegmentUtils.stringToMemorySegment(value);
    }

    @Override
    public String convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return MemorySegmentUtils.memorySegmentToString(value);
    }
}
