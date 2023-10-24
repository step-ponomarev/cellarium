package cellarium.db.converter.types;

import cellarium.db.converter.Converter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public interface MemorySegmentConverter<T> extends Converter<T, MemorySegment> {
    Arena ARENA_OF_AUTO = Arena.ofAuto();
}
