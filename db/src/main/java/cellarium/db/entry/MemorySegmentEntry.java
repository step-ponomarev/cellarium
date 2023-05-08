package cellarium.db.entry;

import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentEntry extends AbstractEntry<MemorySegment> {
    private final long timestamp;
    private final long sizeBytes;

    public MemorySegmentEntry(MemorySegment key, MemorySegment value, long timestamp) {
        super(key, value);
        this.timestamp = timestamp;

        // Key + value + timestamp
        this.sizeBytes = key.byteSize() + (value == null ? 0 : value.byteSize()) + Long.BYTES;
    }

    public long getSizeBytes() {
        return sizeBytes;
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
