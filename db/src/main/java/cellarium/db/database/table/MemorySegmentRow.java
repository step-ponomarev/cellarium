package cellarium.db.database.table;

import cellarium.db.database.types.AValue;
import cellarium.db.database.types.MemorySegmentValue;

import java.util.Map;

public final class MemorySegmentRow extends TableRow<MemorySegmentValue> {
    private final int version;
    private final long timestamp;

    public MemorySegmentRow(MemorySegmentValue pk,
                            Map<String, AValue<?>> columns,
                            long sizeBytes,
                            int version,
                            long timestamp) {
        super(pk, columns, sizeBytes);
        this.version = version;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getVersion() {
        return version;
    }
}
