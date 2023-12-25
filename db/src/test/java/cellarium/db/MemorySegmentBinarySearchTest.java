package cellarium.db;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.Assert;
import org.junit.Test;

import static cellarium.db.MemorySegmentUtils.findIndexOfKey;

public class MemorySegmentBinarySearchTest {
    @Test
    public void testSingleKey() {
        // Инициализация
        int[] offsets = {0};
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
}
