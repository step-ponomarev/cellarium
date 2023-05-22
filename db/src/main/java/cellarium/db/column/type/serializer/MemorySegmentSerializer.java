package cellarium.db.column.type.serializer;

import jdk.incubator.foreign.MemorySegment;

public interface MemorySegmentSerializer<V> extends Serializer<V, MemorySegment> {}