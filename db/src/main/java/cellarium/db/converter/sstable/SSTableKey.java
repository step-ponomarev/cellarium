package cellarium.db.converter.sstable;

import java.lang.foreign.MemorySegment;

import cellarium.db.database.types.DataType;
import cellarium.db.sstable.MemorySegmentValue;

public final class SSTableKey extends MemorySegmentValue {
    public final DataType type;

    public SSTableKey(MemorySegment memorySegment, DataType type) {
        super(memorySegment);

        if (memorySegment == null || type == null) {
            throw new NullPointerException("Cannot be null");
        }

        this.type = type;
    }
}
