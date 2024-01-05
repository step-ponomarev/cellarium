package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

final class ByteMemorySegmentComparator extends AMemorySegmentComparator {
    static final ByteMemorySegmentComparator INSTANCE = new ByteMemorySegmentComparator();

    @Override
    protected int compareWithType(MemorySegment o1, MemorySegment o2, long missMatch) {
        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, missMatch),
                o2.get(ValueLayout.JAVA_BYTE, missMatch)
        );
    }
}
