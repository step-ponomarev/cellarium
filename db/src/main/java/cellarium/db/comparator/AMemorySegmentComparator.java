package cellarium.db.comparator;


import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public abstract class AMemorySegmentComparator implements Comparator<MemorySegment> {
    //TODO: На каждый тип данных нужен свой компаратор, например Read byte не подходит для интов, лонгов и т.д..

    protected AMemorySegmentComparator() {}

    @Override
    public final int compare(MemorySegment o1, MemorySegment o2) {
        if (o1 == null || o2 == null) {
            throw new NullPointerException("Null argument");
        }

        if (o1.byteSize() > o2.byteSize()) {
            return 1;
        }

        if (o1.byteSize() < o2.byteSize()) {
            return -1;
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


        return compareWithType(o1, o2, missMatch);
    }

    protected abstract int compareWithType(MemorySegment o1, MemorySegment o2, long missMatch);
}
