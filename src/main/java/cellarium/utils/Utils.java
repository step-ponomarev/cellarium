package cellarium.utils;

import java.nio.charset.StandardCharsets;
import jdk.incubator.foreign.MemorySegment;

public class Utils {
    private Utils() {}

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
}
