package cellarium.db;

import jdk.incubator.foreign.MemorySegment;

import java.nio.charset.StandardCharsets;

public class MemorySegmentUtils {
    private MemorySegmentUtils() {}

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