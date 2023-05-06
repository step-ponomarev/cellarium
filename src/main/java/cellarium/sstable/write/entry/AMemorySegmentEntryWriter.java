package cellarium.sstable.write.entry;

import java.nio.ByteOrder;
import cellarium.entry.MemorySegmentEntry;
import cellarium.sstable.write.AWriter;

public abstract class AMemorySegmentEntryWriter extends AWriter<MemorySegmentEntry> {
    protected final long tombstoneTag;

    protected AMemorySegmentEntryWriter(long tombstoneTag, ByteOrder byteOrder) {
        super(byteOrder);
        this.tombstoneTag = tombstoneTag;
    }
}
