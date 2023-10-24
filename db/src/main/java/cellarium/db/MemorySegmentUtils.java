package cellarium.db;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class MemorySegmentUtils {
    public static final Arena ARENA_OF_AUTO = Arena.ofAuto();

    private MemorySegmentUtils() {}

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
}
