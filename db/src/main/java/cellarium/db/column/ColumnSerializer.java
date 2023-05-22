package cellarium.db.column;

import java.util.Map;
import cellarium.db.column.type.ColumnType;
import cellarium.db.column.type.serializer.BooleanSerializer;
import cellarium.db.column.type.serializer.IntegerSerializer;
import cellarium.db.column.type.serializer.MemorySegmentSerializer;
import cellarium.db.column.type.serializer.StringSerializer;
import jdk.incubator.foreign.MemorySegment;

public final class ColumnSerializer {
    private final Map<ColumnType, MemorySegmentSerializer<?>> serializers = Map.of(
                ColumnType.STRING,
                new StringSerializer(),
                ColumnType.INTEGER,
                integerSerializer,
                ColumnType.UNSIGNED_INTEGER,
                integerSerializer,
                ColumnType.BOOLEAN,
                new BooleanSerializer()
        );;

    public ColumnSerializer(Map<ColumnType, MemorySegmentSerializer<?>> serializers) {
        final IntegerSerializer integerSerializer = new IntegerSerializer();
        
    }

    public <T> MemorySegment serialize(ColumnType type, T value) {
        final MemorySegmentSerializer<T> serializer = (MemorySegmentSerializer<T>) serializers.get(type);
        return serializer.serialize(value);
    }

    public <T> T deserialize(ColumnType type, MemorySegment value) {
        final MemorySegmentSerializer<T> serializer = (MemorySegmentSerializer<T>) serializers.get(type);
        return serializer.deserialize(value);
    }
}
