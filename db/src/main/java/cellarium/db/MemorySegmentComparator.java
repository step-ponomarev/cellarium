package cellarium.db;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
                o1.get(ValueLayout.JAVA_BYTE, missMatch),
                o2.get(ValueLayout.JAVA_BYTE, missMatch)
        );
    }
}
