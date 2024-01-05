package cellarium.db.comparator;

import java.util.EnumMap;
import java.util.Map;

import cellarium.db.converter.Converter;
import cellarium.db.database.types.DataType;

public final class ComparatorFactory {

    private static final Map<DataType, AMemorySegmentComparator> COMPARATORS = new EnumMap<>(DataType.class);

    static {
        COMPARATORS.put(DataType.LONG, LongMemorySegmentComparator.INSTANCE);
        COMPARATORS.put(DataType.INTEGER, IntegerMemorySegmentComparator.INSTANCE);
        COMPARATORS.put(DataType.BOOLEAN, ByteMemorySegmentComparator.INSTANCE);
        COMPARATORS.put(DataType.STRING, ByteMemorySegmentComparator.INSTANCE);
    }

    private ComparatorFactory() {}

    public static AMemorySegmentComparator getComparator(DataType dataType) {
        final AMemorySegmentComparator comparator = COMPARATORS.get(dataType);
        if (comparator == null) {
            throw new IllegalArgumentException("Unsupported comparator for type: " + dataType);
        }

        return comparator;
    }
}
