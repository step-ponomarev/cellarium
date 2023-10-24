package cellarium.db.converter;

import cellarium.db.database.types.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.foreign.MemorySegment;

public final class MemorySegmentConverterTest {
    @Test
    public void testBooleanConverting() {
        final Converter<Boolean, MemorySegment> converter = ConverterFactory.getConverter(DataType.BOOLEAN);

        MemorySegment convertedValue = converter.convert(Boolean.TRUE);
        Assert.assertTrue(converter.convertBack(convertedValue));

        convertedValue = converter.convert(Boolean.FALSE);
        Assert.assertFalse(converter.convertBack(convertedValue));

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }

    @Test
    public void testIntegerConverter() {
        final Converter<Integer, MemorySegment> converter = ConverterFactory.getConverter(DataType.INTEGER);
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE - 1_000; i += 1_000) {
            final MemorySegment convertedValue = converter.convert(i);
            Assert.assertEquals(i, (int) converter.convertBack(convertedValue));
        }

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }

    @Test
    public void testLongConverter() {
        final Converter<Long, MemorySegment> converter = ConverterFactory.getConverter(DataType.LONG);

        MemorySegment convertedValue = converter.convert(Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, (long) converter.convertBack(convertedValue));

        convertedValue = converter.convert(Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, (long) converter.convertBack(convertedValue));

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }

    @Test
    public void testStringConverter() {
        final Converter<String, MemorySegment> converter = ConverterFactory.getConverter(DataType.STRING);

        final String randomStr = "random";
        Assert.assertEquals(
                randomStr,
                converter.convertBack(
                        converter.convert(randomStr)
                )
        );

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }
}
