package cellarium.db;

import org.junit.Assert;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ThreadLocalRandom;

public final class MemorySegmentUtilsTest {
    @Test
    public void testStringConvert() {
        final String sourceString = "Test string";

        final MemorySegment memorySegment = MemorySegmentUtils.stringToMemorySegment(sourceString);
        Assert.assertEquals(sourceString, MemorySegmentUtils.memorySegmentToString(memorySegment));
    }

    @Test
    public void testSimpleWriteReadMemorySegment() {
        final MemorySegment value = MemorySegmentUtils.stringToMemorySegment("test");
        final MemorySegment target = Arena.ofAuto().allocate(value.byteSize() + Long.BYTES);

        MemorySegmentUtils.writeValue(target, 0, value);

        final MemorySegment readValue = MemorySegmentUtils.readValue(target, 0);
        Assert.assertEquals(0, MemorySegmentComparator.INSTANCE.compare(value, readValue));
    }

    @Test
    public void testWithRandomOffsetWriteReadMemorySegment() {
        final MemorySegment value = MemorySegmentUtils.stringToMemorySegment("test");
        final long oneEntitySize = value.byteSize() + Long.BYTES;

        final ThreadLocalRandom current = ThreadLocalRandom.current();
        final long allocated = current.nextLong(oneEntitySize, 8 * 1024 * 1024);
        final MemorySegment target = Arena.ofAuto().allocate(
                allocated
        );

        final long offset = current.nextLong(0, allocated - oneEntitySize - 1);
        long alignedOffset = offset & ~7L; // три младшие бита зануляем
        MemorySegmentUtils.writeValue(target, alignedOffset, value);


        final MemorySegment readValue = MemorySegmentUtils.readValue(target, alignedOffset);
        Assert.assertEquals(0, MemorySegmentComparator.INSTANCE.compare(value, readValue));
    }
}
