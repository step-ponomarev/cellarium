package cellarium.sstable.write.entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import cellarium.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemorySegment;

public final class FileChannelEntryWriter extends AMemorySegmentEntryWriter {
    private final FileChannel fileChannel;

    public FileChannelEntryWriter(FileChannel fileChannel, long tombstoneTag, ByteOrder byteOrder) {
        super(tombstoneTag, byteOrder);
        this.fileChannel = fileChannel;
    }

    @Override
    public long write(MemorySegmentEntry entry) throws IOException {
        final long startPosition = fileChannel.position();

        long position = startPosition;
        final MemorySegment key = entry.getKey();

        final List<ByteBuffer> buffersToWrite = new ArrayList<>();
        buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).order(byteOrder).putLong(key.byteSize()).flip());
        position += Long.BYTES;

        buffersToWrite.add(key.asByteBuffer());
        position += key.byteSize();

        buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).order(byteOrder).putLong(entry.getTimestamp()).flip());
        position += Long.BYTES;

        final MemorySegment value = entry.getValue();
        if (value == null) {
            buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).order(byteOrder).putLong(tombstoneTag).flip());
            position += Long.BYTES;

            fileChannel.write(buffersToWrite.toArray(ByteBuffer[]::new));

            return position - startPosition;
        }

        buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).order(byteOrder).putLong(value.byteSize()).flip());
        position += Long.BYTES;

        buffersToWrite.add(value.asByteBuffer());
        position += value.byteSize();

        fileChannel.write(buffersToWrite.toArray(ByteBuffer[]::new));

        return position - startPosition;
    }
}
