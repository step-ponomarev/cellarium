package db.sstable.write;

import java.nio.ByteOrder;

public abstract class AWriter<T> implements Writer<T> {
    protected final ByteOrder byteOrder;
    protected long position;

    protected AWriter(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        this.position = 0;
    }
}
