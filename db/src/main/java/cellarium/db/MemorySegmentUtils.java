package cellarium.db;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class MemorySegmentUtils {
    public static final Arena ARENA_OF_AUTO = Arena.ofAuto();

    private MemorySegmentUtils() {
    }

    public static String memorySegmentToString(MemorySegment data) {
        if (data == null) {
            return null;
        }

        return data.getUtf8String(0);
    }

    public static MemorySegment stringToMemorySegment(String data) {
        if (data == null) {
            return null;
        }

        return ARENA_OF_AUTO.allocateUtf8String(data);
    }

    /**
     * @param indexSegment
     * @param key
     * @return index of offset for index memory segment if found, otherwise -1
     */
    public static int findIndexOfKey(MemorySegment indexSegment, int[] indexOffsets, MemorySegment key) {
        int left = 0;
        int right = indexOffsets.length - 1;
        while (left <= right) {
            final int i = left + (right - left) / 2;

            final MemorySegment current = readValue(indexSegment, indexOffsets[i]);
            final int compare = MemorySegmentComparator.INSTANCE.compare(key, current);
            if (compare < 0) {
                right = i - 1;
                continue;
            }

            if (compare > 0) {
                left = i + 1;
                continue;
            }

            return i;
        }

        return -1;
    }

    public static MemorySegment readValue(MemorySegment segment, long offset) {
        final long valueSize = segment.get(ValueLayout.JAVA_LONG, offset);
        return segment.asSlice(offset + Long.BYTES, valueSize);
    }

    public static void writeValue(MemorySegment target, long offset, MemorySegment value) {
        target.set(ValueLayout.JAVA_LONG, offset, value.byteSize());
        target.asSlice(offset + Long.BYTES, value.byteSize()).copyFrom(value);
    }
}
