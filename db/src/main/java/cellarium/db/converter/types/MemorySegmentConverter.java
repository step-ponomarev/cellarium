package cellarium.db.converter.types;

import cellarium.db.converter.Converter;
import jdk.incubator.foreign.MemorySegment;

public interface MemorySegmentConverter<T> extends Converter<T, MemorySegment> {}
