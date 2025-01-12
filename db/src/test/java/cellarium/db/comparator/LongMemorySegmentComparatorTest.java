package cellarium.db.comparator;

import java.lang.foreign.MemorySegment;

import cellarium.db.converter.types.LongMemorySegmentConverter;

public class LongMemorySegmentComparatorTest extends AComparatorTest {
    private static final LongMemorySegmentComparator COMPARATOR = LongMemorySegmentComparator.INSTANCE;
    private static final LongMemorySegmentConverter CONVERTER = LongMemorySegmentConverter.INSTANCE;

    public LongMemorySegmentComparatorTest() {
        super(COMPARATOR);
    }

    @Override
    public MemorySegment createValue1() {
        return CONVERTER.convert(-1000L);
    }

    @Override
    public MemorySegment createValue2() {
        return CONVERTER.convert(0L);
    }

    @Override
    public MemorySegment createValue3() {
        return CONVERTER.convert(11L);
    }
}
