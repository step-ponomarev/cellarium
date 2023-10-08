package cellarium.db.converter.types;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;

public final class IntegerConverterMemorySegmentConverter implements MemorySegmentConverter<Integer> {
    public static final IntegerConverterMemorySegmentConverter INSTANCE = new IntegerConverterMemorySegmentConverter();

    private IntegerConverterMemorySegmentConverter() {}

    @Override
    public MemorySegment convert(Integer value) {
        if (value == null) {
            return null;
        }

        final MemorySegment memorySegment = MemorySegment.ofByteBuffer(
                ByteBuffer.allocate(Integer.BYTES)
        );

        MemoryAccess.setInt(memorySegment, value);
        return memorySegment;
    }

    @Override
    public Integer convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return MemoryAccess.getInt(value);
    }
}
