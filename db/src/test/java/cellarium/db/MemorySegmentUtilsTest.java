package cellarium.db;

import org.junit.Assert;
import org.junit.Test;
import jdk.incubator.foreign.MemorySegment;

public final class MemorySegmentUtilsTest {

    @Test
    public void testStringConvertion() {
        final String sourceString = "Test string";

        final MemorySegment memorySegment = MemorySegmentUtils.stringToMemorySegment(sourceString);
        Assert.assertEquals(sourceString, MemorySegmentUtils.memorySegmentToString(memorySegment));
    }
}
