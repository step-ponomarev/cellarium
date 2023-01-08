package test.entry;

import java.nio.charset.StandardCharsets;
import jdk.incubator.foreign.MemorySegment;
import cellarium.entry.Entry;

public class Utils {
    private Utils() {
    }

    public static String generateKeyByIndex(int index) {
        return String.format("%012d", index);
    }

    public static String memorySegmentToString(MemorySegment data) {
        if (data == null) {
            return null;
        }

        return StandardCharsets.UTF_8.decode(data.asByteBuffer()).toString();
    }

    public static MemorySegment stringToMemorySegment(String data) {
        if (data == null) {
            return null;
        }

        return MemorySegment.ofByteBuffer(StandardCharsets.UTF_8.encode(data));
    }

    public static long getSizeBytesOf(Entry<String> entry) {
        return Utils.stringToMemorySegment(entry.getKey()).byteSize() +
                (entry.getValue() == null ? 0 : Utils.stringToMemorySegment(entry.getValue()).byteSize());
    }
}
