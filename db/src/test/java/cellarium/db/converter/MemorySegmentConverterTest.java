package cellarium.db.converter;

import cellarium.db.database.types.DataType;
import jdk.incubator.foreign.MemorySegment;
import org.junit.Assert;
import org.junit.Test;

public final class MemorySegmentConverterTest {
    @Test
    public void testBooleanConverting() {
        final ColumnConverter<Boolean, MemorySegment> converter = ConverterFactory.getConverter(DataType.BOOLEAN);

        MemorySegment convertedValue = converter.convert(Boolean.TRUE);
        Assert.assertTrue(converter.convertBack(convertedValue));

        convertedValue = converter.convert(Boolean.FALSE);
        Assert.assertFalse(converter.convertBack(convertedValue));

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }

    @Test
    public void testIntegerConverter() {
        final ColumnConverter<Integer, MemorySegment> converter = ConverterFactory.getConverter(DataType.INTEGER);
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE - 1_000; i += 1_000) {
            final MemorySegment convertedValue = converter.convert(i);
            Assert.assertEquals(i, (int) converter.convertBack(convertedValue));
        }

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }

    @Test
    public void testLongConverter() {
        final ColumnConverter<Long, MemorySegment> converter = ConverterFactory.getConverter(DataType.LONG);

        final long step = 10_000_000_000l;
        for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE - step; i += step) {
            final MemorySegment convertedValue = converter.convert(i);
            Assert.assertEquals(i, (long) converter.convertBack(convertedValue));
        }

        Assert.assertNull(converter.convert(null));
        Assert.assertNull(converter.convertBack(null));
    }

    @Test
    public void testStringConverter() {
        final ColumnConverter<String, MemorySegment> converter = ConverterFactory.getConverter(DataType.STRING);

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
