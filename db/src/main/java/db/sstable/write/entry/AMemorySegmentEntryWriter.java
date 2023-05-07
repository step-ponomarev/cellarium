package db.sstable.write.entry;

import java.nio.ByteOrder;
import db.entry.MemorySegmentEntry;
import db.sstable.write.AWriter;

public abstract class AMemorySegmentEntryWriter extends AWriter<MemorySegmentEntry> {
    protected final long tombstoneTag;

    protected AMemorySegmentEntryWriter(long tombstoneTag, ByteOrder byteOrder) {
        super(byteOrder);
        this.tombstoneTag = tombstoneTag;
    }
}
