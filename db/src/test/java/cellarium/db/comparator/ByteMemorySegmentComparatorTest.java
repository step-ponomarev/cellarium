package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;

import cellarium.db.converter.types.StringMemorySegmentConverter;

public final class ByteMemorySegmentComparatorTest extends AComparatorTest {
    private static final ByteMemorySegmentComparator COMPARATOR = ByteMemorySegmentComparator.INSTANCE;
    private static final StringMemorySegmentConverter CONVERTER = StringMemorySegmentConverter.INSTANCE;

    public ByteMemorySegmentComparatorTest() {
        super(COMPARATOR);
    }

    @Override
    public MemorySegment createValue1() {
        return CONVERTER.convert("str1");
    }

    @Override
    public MemorySegment createValue2() {
        return CONVERTER.convert("str2");
    }

    @Override
    public MemorySegment createValue3() {
        return CONVERTER.convert("str3");
    }
}
