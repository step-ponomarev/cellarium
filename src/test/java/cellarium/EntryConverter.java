package cellarium;

import cellarium.entry.AbstractEntry;
import cellarium.entry.Entry;
import cellarium.entry.MemorySegmentEntry;
import test.entry.Utils;

public final class EntryConverter {
    private EntryConverter() {}

    public static MemorySegmentEntry convert(Entry<String> entry) {
        if (entry == null) {
            return null;
        }

        return new MemorySegmentEntry(
                Utils.stringToMemorySegment(entry.getKey()),
                entry.getValue() == null ? null : Utils.stringToMemorySegment(entry.getValue()),
                System.currentTimeMillis());
    }

    public static Entry<String> convert(MemorySegmentEntry entry) {
        if (entry == null) {
            return null;
        }

        return new AbstractEntry<>(
                Utils.memorySegmentToString(entry.getKey()),
                Utils.memorySegmentToString(entry.getValue())) {
        };
    }
}
