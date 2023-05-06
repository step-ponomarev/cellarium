package cellarium.disk;

import java.nio.ByteOrder;
import jdk.incubator.foreign.MemorySegment;

public abstract class AMemorySegmentHandler {
    protected static final ByteOrder STANDART_BYTE_OREDER = ByteOrder.BIG_ENDIAN;
    
    protected final MemorySegment memorySegment;
    protected final long tombstoneTag;
    protected long position;

    protected AMemorySegmentHandler(MemorySegment memorySegment, long tombstoneTag) {
        if (memorySegment == null) {
            throw new NullPointerException("Memory segment cannot be null");
        }

        this.memorySegment = memorySegment;
        this.tombstoneTag = tombstoneTag;
        this.position = 0;
    }
}
