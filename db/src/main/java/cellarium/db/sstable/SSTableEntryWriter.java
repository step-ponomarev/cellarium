package cellarium.db.sstable;

import java.io.IOException;
import cellarium.db.sstable.write.entry.AMemorySegmentEntryWriter;
import cellarium.db.entry.MemorySegmentEntry;
import cellarium.db.sstable.write.Writer;

final class SSTableEntryWriter implements Writer<MemorySegmentEntry> {
    private final Writer<Long> indexWriter;
    private final AMemorySegmentEntryWriter memorySegmentEntryWriter;
    private long dataOffset;

    public SSTableEntryWriter(Writer<Long> indexWriter, AMemorySegmentEntryWriter memorySegmentEntryWriter) {
        this.indexWriter = indexWriter;
        this.memorySegmentEntryWriter = memorySegmentEntryWriter;
        this.dataOffset = 0;
    }

    @Override
    public long write(MemorySegmentEntry value) throws IOException {
        final long size = memorySegmentEntryWriter.write(value);

        indexWriter.write(dataOffset);
        dataOffset += size;

        return size;
    }
}
