package cellarium.db.converter;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.converter.types.StringMemorySegmentConverter;

public class StringMemorySegmentConverterTest {
    @Test
    public void testStringConverting() {
        final String value = "String test";

        final MemorySegment convert = StringMemorySegmentConverter.INSTANCE.convert(value);
        Assert.assertEquals(value, StringMemorySegmentConverter.INSTANCE.convertBack(convert));
    }
}
