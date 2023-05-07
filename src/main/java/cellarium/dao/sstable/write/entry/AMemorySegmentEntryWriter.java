package cellarium.dao.sstable.write.entry;

import java.nio.ByteOrder;
import cellarium.dao.entry.MemorySegmentEntry;
import cellarium.dao.sstable.write.AWriter;

public abstract class AMemorySegmentEntryWriter extends AWriter<MemorySegmentEntry> {
    protected final long tombstoneTag;

    protected AMemorySegmentEntryWriter(long tombstoneTag, ByteOrder byteOrder) {
        super(byteOrder);
        this.tombstoneTag = tombstoneTag;
    }
}
