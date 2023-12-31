package cellarium.db.sstable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.MemorySegmentUtils;


public class MemorySegmentBinarySearchTest {
    @Test
    public void testSingleKey() {
        final long[] offsets = {0};
        final MemorySegment key = MemorySegmentUtils.stringToMemorySegment("test");
        final MemorySegment indexSegment = Arena.ofAuto().allocate(key.byteSize() + Long.BYTES);

        final long writeOffset = 0;
        MemorySegmentUtils.writeValue(indexSegment, writeOffset, key);

        final int foundIndex = SSTable.findIndexOfKey(
                indexSegment,
                offsets,
                key,
                false
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
        offsets[1] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[2] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);

        final int foundIndex = SSTable.findIndexOfKey(
                indexSegment,
                offsets,
                key3,
                false
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
        offsets[1] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[2] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);

        final int foundIndex = SSTable.findIndexOfKey(
                indexSegment,
                offsets,
                MemorySegmentUtils.stringToMemorySegment("test2"),
                true
        );

        Assert.assertEquals(1, foundIndex);
    }
    
    @Test
    public void testFindPrevKey() {
        final long[] offsets = new long[4];
        final MemorySegment key1 = MemorySegmentUtils.stringToMemorySegment("test1");
        final MemorySegment key2 = MemorySegmentUtils.stringToMemorySegment("test2");
        final MemorySegment key3 = MemorySegmentUtils.stringToMemorySegment("test3");
        final MemorySegment key4 = MemorySegmentUtils.stringToMemorySegment("test6");

        final MemorySegment indexSegment = Arena.ofAuto().allocate(getSegmentsCommonSize(key1, key2, key3, key4) + Long.BYTES * 4);

        long writeOffset = 0;
        offsets[1] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[2] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        offsets[3] = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);

        final int foundIndex = SSTable.findIndexOfKey(
                indexSegment,
                offsets,
                MemorySegmentUtils.stringToMemorySegment("test5"),
                false
        );

        Assert.assertEquals(2, foundIndex);
    }

    @Test
    public void testFindNextMiddleKey() {
        final long[] offsets = new long[5];
        final MemorySegment key1 = MemorySegmentUtils.stringToMemorySegment("test1");
        final MemorySegment key2 = MemorySegmentUtils.stringToMemorySegment("test2");
        final MemorySegment key3 = MemorySegmentUtils.stringToMemorySegment("test3");
        final MemorySegment key4 = MemorySegmentUtils.stringToMemorySegment("test6");
        final MemorySegment key5 = MemorySegmentUtils.stringToMemorySegment("test7");

        final MemorySegment indexSegment = Arena.ofAuto().allocate(getSegmentsCommonSize(key1, key2, key3, key4, key5) + Long.BYTES * 5);

        long writeOffset = 0;
        offsets[1] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[2] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        offsets[3] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);
        offsets[4] = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key4);

        final int foundIndex = SSTable.findIndexOfKey(
                indexSegment,
                offsets,
                MemorySegmentUtils.stringToMemorySegment("test4"),
                true
        );

        Assert.assertEquals(3, foundIndex);
    }
    
    @Test
    public void testFindPrevMiddleKey() {
        final long[] offsets = new long[5];
        final MemorySegment key1 = MemorySegmentUtils.stringToMemorySegment("test1");
        final MemorySegment key2 = MemorySegmentUtils.stringToMemorySegment("test2");
        final MemorySegment key3 = MemorySegmentUtils.stringToMemorySegment("test3");
        final MemorySegment key4 = MemorySegmentUtils.stringToMemorySegment("test5");
        final MemorySegment key5 = MemorySegmentUtils.stringToMemorySegment("test6");

        final MemorySegment indexSegment = Arena.ofAuto().allocate(getSegmentsCommonSize(key1, key2, key3, key4, key5) + Long.BYTES * 5);

        long writeOffset = 0;
        offsets[1] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key1);
        offsets[2] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key2);
        offsets[3] = writeOffset = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key3);
        offsets[4] = MemorySegmentUtils.writeValue(indexSegment, writeOffset, key4);

        final int foundIndex = SSTable.findIndexOfKey(
                indexSegment,
                offsets,
                MemorySegmentUtils.stringToMemorySegment("test4"),
                false
        );

        Assert.assertEquals(3, foundIndex);
    }

    private static long getSegmentsCommonSize(MemorySegment... segments) {
        long size = 0;
        for (MemorySegment s : segments) {
            size += s.byteSize();
        }

        return size;
    }
}
