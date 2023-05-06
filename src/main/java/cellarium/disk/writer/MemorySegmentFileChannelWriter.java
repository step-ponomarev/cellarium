package cellarium.disk.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import cellarium.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentFileChannelWriter implements Writer<MemorySegmentEntry> {
    private final FileChannel ssTableFileChannel;
    private final long tombstoneTag;

    public MemorySegmentFileChannelWriter(FileChannel ssTableFileChannel, long tombstoneTag) {
        this.ssTableFileChannel = ssTableFileChannel;
        this.tombstoneTag = tombstoneTag;
    }

    @Override
    public long write(MemorySegmentEntry entry) throws IOException {
        final long startPosition = ssTableFileChannel.position();

        long position = startPosition;
        final MemorySegment key = entry.getKey();

        final List<ByteBuffer> buffersToWrite = new ArrayList<>();
        buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).putLong(key.byteSize()).flip());
        position += Long.BYTES;

        buffersToWrite.add(key.asByteBuffer());
        position += key.byteSize();

        buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).putLong(entry.getTimestamp()).flip());
        position += Long.BYTES;

        final MemorySegment value = entry.getValue();
        if (value == null) {
            buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).putLong(tombstoneTag).flip());
            position += Long.BYTES;

            ssTableFileChannel.write(buffersToWrite.toArray(ByteBuffer[]::new));

            return position - startPosition;
        }

        buffersToWrite.add(ByteBuffer.allocate(Long.BYTES).putLong(value.byteSize()).flip());
        position += Long.BYTES;

        buffersToWrite.add(value.asByteBuffer());
        position += value.byteSize();

        ssTableFileChannel.write(buffersToWrite.toArray(ByteBuffer[]::new));

        return position - startPosition;
    }
}
