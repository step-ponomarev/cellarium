package test.entry;

import cellarium.dao.entry.Entry;
import cellarium.dao.utils.Utils;

public class TestUtils {
    private TestUtils() {}

    public static long getSizeBytesOf(Entry<String> entry) {
        return Utils.stringToMemorySegment(entry.getKey()).byteSize() +
                (entry.getValue() == null ? 0 : Utils.stringToMemorySegment(entry.getValue()).byteSize());
    }

    public static String generateKeyByIndex(int index) {
        return String.format("%012d", index);
    }
}
