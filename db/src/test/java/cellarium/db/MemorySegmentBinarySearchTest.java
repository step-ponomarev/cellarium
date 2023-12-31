package cellarium.db;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.Assert;
import org.junit.Test;

import static cellarium.db.MemorySegmentUtils.findIndexOfKey;

public class MemorySegmentBinarySearchTest {
    @Test
    public void testSingleKey() {
        final long[] offsets = {0};
        final MemorySegment key = MemorySegmentUtils.stringToMemorySegment("test");
        final MemorySegment indexSegment = Arena.ofAuto().allocate(key.byteSize() + Long.BYTES);

        final long writeOffset = 0;
        MemorySegmentUtils.writeValue(indexSegment, writeOffset, key);

        final int foundIndex = findIndexOfKey(
                indexSegment,
                offsets,
                key
        );

        Assert.assertEquals(0, foundIndex);
    }

    @Test
    public void testFindExactKey() {
        final long[] offsets = new long[3];
        final MemorySegment key1 = MemorySegmentUtils.stringToMemorySegment("test");
        final MemorySegment key2 = MemorySegmentUtils.stringToMemorySegment("test2");
        final MemorySegment key3 = MemorySegmentUtils.stringToMemorySegment("test3");

        final MemorySegment indexSegment = Arena.ofAuto().allocate(getSegmentsCommonSize(key1, key2, key3) + Long.BYTES * 3);

        long writeOffset = 0;
        offsets[0] = writeOffset;

        writeOffset += MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[1] = writeOffset;

        writeOffset += MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        offsets[2] = writeOffset;

        MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);

        final int foundIndex = findIndexOfKey(
                indexSegment,
                offsets,
                key3
        );

        Assert.assertEquals(2, foundIndex);
    }

    @Test
    public void testFindNextKey() {
        final long[] offsets = new long[3];
        final MemorySegment key1 = MemorySegmentUtils.stringToMemorySegment("test");
        final MemorySegment key2 = MemorySegmentUtils.stringToMemorySegment("test3");
        final MemorySegment key3 = MemorySegmentUtils.stringToMemorySegment("test4");

        final MemorySegment indexSegment = Arena.ofAuto().allocate(getSegmentsCommonSize(key1, key2, key3) + Long.BYTES * 3);

        long writeOffset = 0;
        offsets[0] = writeOffset;

        writeOffset += MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[1] = writeOffset;

        writeOffset += MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        offsets[2] = writeOffset;

        MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);

        final int foundIndex = findIndexOfKey(
                indexSegment,
                offsets,
                MemorySegmentUtils.stringToMemorySegment("test2")
        );

        Assert.assertEquals(1, foundIndex);
    }

    private static long getSegmentsCommonSize(MemorySegment... segments) {
        long size = 0;
        for (MemorySegment s : segments) {
            size += s.byteSize();
        }

        return size;
    }
}
