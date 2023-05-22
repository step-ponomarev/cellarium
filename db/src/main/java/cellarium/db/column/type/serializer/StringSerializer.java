package cellarium.db.column.type.serializer;

import java.nio.charset.StandardCharsets;
import jdk.incubator.foreign.MemorySegment;

public final class StringSerializer implements MemorySegmentSerializer<String> {
    @Override
    public MemorySegment serialize(String value) {
        return MemorySegment.ofByteBuffer(StandardCharsets.UTF_8.encode(value));
    }

    @Override
    public String deserialize(MemorySegment serialized) {
        return StandardCharsets.UTF_8.decode(serialized.asByteBuffer()).toString();
    }
}
