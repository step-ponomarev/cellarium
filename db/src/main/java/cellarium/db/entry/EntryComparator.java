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
