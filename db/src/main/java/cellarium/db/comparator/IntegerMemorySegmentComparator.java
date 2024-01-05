package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

final class IntegerMemorySegmentComparator extends AMemorySegmentComparator {
    static final IntegerMemorySegmentComparator INSTANCE = new IntegerMemorySegmentComparator();

    @Override
    protected int compareWithType(MemorySegment o1, MemorySegment o2, long missMatch) {
        return Integer.compare(
                o1.get(ValueLayout.JAVA_INT_UNALIGNED, missMatch),
                o2.get(ValueLayout.JAVA_INT_UNALIGNED, missMatch)
        );
    }
}
