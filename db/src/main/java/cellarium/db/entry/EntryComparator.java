package cellarium.db.entry;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class EntryComparator {
    private EntryComparator() {
    }

    public static int compareMemorySegments(MemorySegment s1, MemorySegment s2) {
        final long mismatch = s1.mismatch(s2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == s1.byteSize()) {
            return -1;
        }

        if (mismatch == s2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                MemoryAccess.getByteAtOffset(s1, mismatch),
                MemoryAccess.getByteAtOffset(s2, mismatch)
        );
    }

    public static <E extends Entry<MemorySegment>> int compareMemorySegmentEntryKeys(E r1, E r2) {
        if (r1 == null) {
            return 1;
        }

        if (r2 == null) {
            return -1;
        }

        return compareMemorySegments(r1.getKey(), r2.getKey());
    }

    public static <E extends Entry<MemorySegment>> boolean areEquals(E e1, E e2) {
        if (e1 == null || e2 == null) {
            return false;
        }

        if (compareMemorySegmentEntryKeys(e1, e2) != 0) {
            return false;
        }

        if (e1.getValue() == null && e2.getValue() == null) {
            return true;
        }

        return e1.getValue() != null
                && e2.getValue() != null
                && compareMemorySegments(e1.getValue(), e2.getValue()) == 0;
    }

    public static int compareStringEntries(Entry<String> r1, Entry<String> r2) {
        if (r1 == null) {
            return 1;
        }

        if (r2 == null) {
            return -1;
        }

        return r1.getKey().compareTo(r2.getKey());
    }
}
