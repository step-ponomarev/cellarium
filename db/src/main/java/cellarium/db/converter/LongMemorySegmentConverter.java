package cellarium.db.converter;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;

public final class LongMemorySegmentConverter implements MemorySegmentConverter<Long> {
    private static final LongMemorySegmentConverter INSTANCE = new LongMemorySegmentConverter();

    private LongMemorySegmentConverter() {}

    @Override
    public MemorySegment convert(Long value) {
        if (value == null) {
            return null;
        }

        final MemorySegment memorySegment = MemorySegment.ofByteBuffer(
                ByteBuffer.allocate(Long.BYTES)
        );

        MemoryAccess.setLong(memorySegment, value);
        return memorySegment;
    }

    @Override
    public Long convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return MemoryAccess.getLong(value);
    }
}
