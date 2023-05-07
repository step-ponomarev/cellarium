package cellarium.dao.sstable.write.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import cellarium.dao.sstable.write.AWriter;

public final class FileChannelIndexWriter extends AWriter<Long> {
    private final FileChannel fileChannel;

    public FileChannelIndexWriter(FileChannel fileChannel, ByteOrder byteOrder) {
        super(byteOrder);
        this.fileChannel = fileChannel;
    }

    @Override
    public long write(Long value) throws IOException {
        fileChannel.write(
                ByteBuffer.allocate(Long.BYTES).order(byteOrder).putLong(value).flip()
        );

        return Long.BYTES;
    }
}
