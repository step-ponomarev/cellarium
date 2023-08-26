package cellarium.db.converter;

import jdk.incubator.foreign.MemorySegment;

public interface MemorySegmentConverter<T> extends ColumnConverter<T, MemorySegment> {}
