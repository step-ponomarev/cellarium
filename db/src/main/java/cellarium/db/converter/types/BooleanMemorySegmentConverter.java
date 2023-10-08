package cellarium.db.converter.types;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;

public final class BooleanMemorySegmentConverter implements MemorySegmentConverter<Boolean> {
    public static final BooleanMemorySegmentConverter INSTANCE = new BooleanMemorySegmentConverter();

    private BooleanMemorySegmentConverter() {}

    @Override
    public MemorySegment convert(Boolean value) {
        if (value == null) {
            return null;
        }

        final MemorySegment memorySegment = MemorySegment.ofByteBuffer(
                ByteBuffer.allocate(Byte.BYTES)
        );

        MemoryAccess.setByte(memorySegment, (byte) (value ? 1 : 0));
        return memorySegment;
    }

    @Override
    public Boolean convertBack(MemorySegment value) {
        if (value == null) {
            return null;
        }

        return MemoryAccess.getByte(value) == 1;
    }
}
