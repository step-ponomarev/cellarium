package cellarium.db.converter.sstable;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.List;

import cellarium.db.database.types.DataType;
import cellarium.db.sstable.MemorySegmentValue;

public final class SSTableKey extends MemorySegmentValue {
    public final List<DataType> types;

    public SSTableKey(MemorySegment memorySegment, DataType type) {
        this(memorySegment, List.of(type));
    }

    public SSTableKey(MemorySegment memorySegment, List<DataType> types) {
        super(memorySegment);

        if (memorySegment == null || types == null || types.isEmpty()) {
            throw new NullPointerException("Cannot be null");
        }

        this.types = Collections.unmodifiableList(types);
    }
}
