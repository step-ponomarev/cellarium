package cellarium.db.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;


public final class SSTable implements Closeable {
    // четко знает свой размер в байтах
    // есть метаданные
    // есть индекс

    private final MemorySegment indexSegment; //key, entity offset
    private final int[] indexOffsets;

    private final MemorySegment dataSegment;

    SSTable(MemorySegment indexSegment, int[] indexOffsets, MemorySegment dataSegment) {
        this.indexSegment = indexSegment;
        this.indexOffsets = indexOffsets;

        this.dataSegment = dataSegment;
    }

    private MemorySegment find(MemorySegment from, MemorySegment to) {
        


        return null;
    }

    @Override
    public void close() throws IOException {
//        this.indexSegment.scope().close();
//        this.dataSegment.scope().close();
    }
}
