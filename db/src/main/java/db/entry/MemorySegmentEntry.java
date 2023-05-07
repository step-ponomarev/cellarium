package db.entry;

import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentEntry extends AbstractEntry<MemorySegment> {
    private final long timestamp;

    public MemorySegmentEntry(MemorySegment key, MemorySegment value, long timestamp) {
        super(key, value);
        this.timestamp = timestamp;
    }

    public long getSizeBytes() {
        // Key + value + timestamp
        return key.byteSize() + (value == null ? 0 : value.byteSize()) + Long.BYTES;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public MemorySegment getKey() {
        return key;
    }

    @Override
    public MemorySegment getValue() {
        return value;
    }
}
