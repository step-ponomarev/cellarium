package cellarium.dao.utils;

import java.nio.charset.StandardCharsets;
import cellarium.dao.entry.AbstractEntry;
import cellarium.dao.entry.Entry;
import cellarium.dao.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemorySegment;

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

    public static MemorySegmentEntry convert(Entry<String> entry) {
        if (entry == null) {
            return null;
        }

        return new MemorySegmentEntry(
                MemorySegmentUtils.stringToMemorySegment(entry.getKey()),
                entry.getValue() == null ? null : MemorySegmentUtils.stringToMemorySegment(entry.getValue()),
                System.currentTimeMillis());
    }

    public static Entry<String> convert(MemorySegmentEntry entry) {
        if (entry == null) {
            return null;
        }

        return new AbstractEntry<>(
                MemorySegmentUtils.memorySegmentToString(entry.getKey()),
                MemorySegmentUtils.memorySegmentToString(entry.getValue())) {
        };
    }
}
