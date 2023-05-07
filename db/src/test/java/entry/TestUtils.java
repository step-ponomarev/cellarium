package entry;

import cellarium.db.entry.Entry;
import cellarium.db.utils.MemorySegmentUtils;

public class TestUtils {
    private TestUtils() {}

    public static long getSizeBytesOf(Entry<String> entry) {
        return MemorySegmentUtils.stringToMemorySegment(entry.getKey()).byteSize() +
                (entry.getValue() == null ? 0 : MemorySegmentUtils.stringToMemorySegment(entry.getValue()).byteSize());
    }

    public static String generateKeyByIndex(int index) {
        return String.format("%012d", index);
    }
}
