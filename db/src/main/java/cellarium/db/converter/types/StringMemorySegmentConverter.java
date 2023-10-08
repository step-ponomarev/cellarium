package cellarium.db.converter.types;

import jdk.incubator.foreign.MemorySegment;

import java.nio.charset.StandardCharsets;

public final class StringMemorySegmentConverter implements MemorySegmentConverter<String> {
    public static final StringMemorySegmentConverter INSTANCE = new StringMemorySegmentConverter();

    @Override
    public MemorySegment convert(String value) {
        if (value == null) {
            return null;
        }

        return MemorySegment.ofByteBuffer(
                StandardCharsets.UTF_8.encode(value)
        );
    }

    @Override
    public String convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return StandardCharsets.UTF_8.decode(value.asByteBuffer()).toString();
    }
}
