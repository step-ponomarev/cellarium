package cellarium.db.column.type.serializer;

import java.nio.ByteBuffer;
import jdk.incubator.foreign.MemorySegment;

public final class BooleanSerializer implements MemorySegmentSerializer<Boolean> {
    private static final BooleanSerializer INSTANCE = new BooleanSerializer();
    
    private BooleanSerializer() {}

    @Override
    public MemorySegment serialize(Boolean value) {
        final ByteBuffer bb = ByteBuffer.allocate(1);
        bb.put((byte) (value ? 1 : 0));
        bb.flip();

        return MemorySegment.ofByteBuffer(bb);
    }

    @Override
    public Boolean deserialize(MemorySegment serialized) {
        if (serialized.byteSize() > 1) {
            throw new IllegalStateException("Invalid byte size: " + serialized.byteSize() + ", expected: 1");
        }

        final byte[] bytes = serialized.toByteArray();
        if (bytes[0] == 1) {
            return true;
        }

        if (bytes[0] == 0) {
            return false;
        }

        throw new IllegalStateException("Unrecognized value");
    }
}
