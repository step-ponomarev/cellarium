package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.MemorySegmentComparator;
import cellarium.db.MemorySegmentUtils;
import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.BooleanValue;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.LongValue;
import cellarium.db.database.types.StringValue;

public class SSTableValueConverterTest {
    @Test
    public void testStringConverting() {
        final StringValue source = StringValue.of("Test string value");
        final MemorySegment value = SSTableValueConverter.INSTANCE.convert(source);

        final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(value);
        Assert.assertEquals(source.getValue(), aValue.getValue());
        Assert.assertEquals(source.getSizeBytes(), aValue.getSizeBytes());
        Assert.assertEquals(source.getDataType(), aValue.getDataType());
    }

    @Test
    public void testBooleanConverting() {
        final BooleanValue source = BooleanValue.of(true);
        final MemorySegment converted = SSTableValueConverter.INSTANCE.convert(source);

        final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(converted);
        Assert.assertEquals(source.getValue(), aValue.getValue());
        Assert.assertEquals(source.getSizeBytes(), aValue.getSizeBytes());
        Assert.assertEquals(source.getDataType(), aValue.getDataType());
    }

    @Test
    public void testLongConverting() {
        final LongValue source = LongValue.of(Long.MAX_VALUE);
        final MemorySegment converted = SSTableValueConverter.INSTANCE.convert(source);

        final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(converted);
        Assert.assertEquals(source.getValue(), aValue.getValue());
        Assert.assertEquals(source.getSizeBytes(), aValue.getSizeBytes());
        Assert.assertEquals(source.getDataType(), aValue.getDataType());
    }

    @Test
    public void testIntegerConverting() {
        final IntegerValue source = IntegerValue.of(Integer.MAX_VALUE);
        final MemorySegment converted = SSTableValueConverter.INSTANCE.convert(source);

        final AValue<?> aValue = SSTableValueConverter.INSTANCE.convertBack(converted);
        Assert.assertEquals(source.getValue(), aValue.getValue());
        Assert.assertEquals(source.getSizeBytes(), aValue.getSizeBytes());
        Assert.assertEquals(source.getDataType(), aValue.getDataType());
    }

    @Test
    public void testReadDbValue() {
        final IntegerValue source = IntegerValue.of(Integer.MAX_VALUE);
        final MemorySegment converted = SSTableValueConverter.INSTANCE.convert(source);
        MemorySegment memorySegment = MemorySegmentUtils.sliceFirstDbValue(converted);

        int compare = MemorySegmentComparator.INSTANCE.compare(converted, memorySegment);
        Assert.assertEquals(0, compare);
    }
}
