package cellarium.db;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

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
