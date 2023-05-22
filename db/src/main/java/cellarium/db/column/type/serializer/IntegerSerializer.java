package cellarium.db.column.type.serializer;

import java.nio.ByteBuffer;
import jdk.incubator.foreign.MemorySegment;

public final class IntegerSerializer implements MemorySegmentSerializer<Integer> {
    private static final IntegerSerializer INSTANCE = new IntegerSerializer();
    
    private IntegerSerializer() {}

    @Override
    public MemorySegment serialize(Integer value) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();

        return MemorySegment.ofByteBuffer(byteBuffer);
    }

    @Override
    public Integer deserialize(MemorySegment serialized) {
        return serialized.asByteBuffer().getInt();
    }
}
