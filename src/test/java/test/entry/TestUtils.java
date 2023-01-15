package test.entry;

import cellarium.entry.Entry;

public class TestUtils {
    private TestUtils() {}

    public static long getSizeBytesOf(Entry<String> entry) {
        return cellarium.utils.Utils.stringToMemorySegment(entry.getKey()).byteSize() +
                (entry.getValue() == null ? 0 : cellarium.utils.Utils.stringToMemorySegment(entry.getValue()).byteSize());
    }

    public static String generateKeyByIndex(int index) {
        return String.format("%012d", index);
    }
}
