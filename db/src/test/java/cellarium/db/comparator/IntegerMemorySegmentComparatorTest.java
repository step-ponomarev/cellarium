package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;

import cellarium.db.converter.types.IntegerMemorySegmentConverter;

public class IntegerMemorySegmentComparatorTest extends AComparatorTest {
    private static final IntegerMemorySegmentComparator COMPARATOR = IntegerMemorySegmentComparator.INSTANCE;
    private static final IntegerMemorySegmentConverter CONVERTER = IntegerMemorySegmentConverter.INSTANCE;

    public IntegerMemorySegmentComparatorTest() {
        super(COMPARATOR);
    }

    @Override
    public MemorySegment createValue1() {
        return CONVERTER.convert(1);
    }

    @Override
    public MemorySegment createValue2() {
        return CONVERTER.convert(2);
    }

    @Override
    public MemorySegment createValue3() {
        return CONVERTER.convert(3);
    }
}
