package cellarium.db;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {
    public static final MemorySegmentComparator INSTANCE = new MemorySegmentComparator();

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        if (o1 == null || o2 == null) {
            throw new NullPointerException("Null argument");
        }

        final long missMatch = o1.mismatch(o2);
        if (missMatch == -1) {
            return 0;
        }

        if (missMatch >= o1.byteSize()) {
            return -1;
        }

        if (missMatch >= o2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                MemoryAccess.getByteAtOffset(o1, missMatch),
                MemoryAccess.getByteAtOffset(o2, missMatch)
        );
    }
}
