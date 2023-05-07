package db.store;

import java.util.Iterator;
import db.entry.MemorySegmentEntry;

public class FlushData {
    public final Iterator<MemorySegmentEntry> data;
    public final int count;
    public final long sizeBytes;

    public FlushData(Iterator<MemorySegmentEntry> data, int count, long sizeBytes) {
        this.data = data;
        this.count = count;
        this.sizeBytes = sizeBytes;
    }
}
