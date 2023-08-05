package cellarium.db.converter;

import jdk.incubator.foreign.MemorySegment;

public abstract class MemorySegmentConverter<T> implements ColumnConverter<T, MemorySegment> {}
