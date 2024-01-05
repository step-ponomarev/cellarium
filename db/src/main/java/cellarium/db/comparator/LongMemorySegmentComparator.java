package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

final class LongMemorySegmentComparator extends AMemorySegmentComparator {
    static final LongMemorySegmentComparator INSTANCE = new LongMemorySegmentComparator();

    @Override
    protected int compareWithType(MemorySegment o1, MemorySegment o2, long missMatch) {
        return Long.compare(
                o1.get(ValueLayout.JAVA_LONG_UNALIGNED, missMatch),
                o2.get(ValueLayout.JAVA_LONG_UNALIGNED, missMatch)
        );
    }
}
