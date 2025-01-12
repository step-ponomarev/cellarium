package cellarium.db;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collection;
import java.util.Comparator;

import cellarium.db.converter.Converter;
import cellarium.db.converter.ConverterFactory;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.BooleanValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.LongValue;
import cellarium.db.database.types.StringValue;
import cellarium.db.sstable.DataMemorySegmentValue;
import cellarium.db.sstable.IndexMemorySegmentValue;

public final class MemorySegmentUtils {
    public static final Arena ARENA_OF_AUTO = Arena.ofAuto();
    public static final byte BYTE_SIZE = 1;

    private MemorySegmentUtils() {}

    public static AValue<?> toValue(DataType dataType, MemorySegment value) {
        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);

        return switch (dataType) {
            case INTEGER -> IntegerValue.of((Integer) converter.convertBack(value));
            case LONG -> LongValue.of((Long) converter.convertBack(value));
            case BOOLEAN -> BooleanValue.of((Boolean) converter.convertBack(value));
            case STRING -> StringValue.of((String) converter.convertBack(value));
            default -> throw new IllegalStateException("Unsupported data type");
        };
    }

    //TODO: На момент обращения мы можем выяснить тип данных и читать сразу его,
    // а не анализировать каждую запись
    public static MemorySegment sliceFirstDbValue(MemorySegment value) {
        final byte typeId = value.get(ValueLayout.JAVA_BYTE, 0);
        final DataType dataType = DataType.getById(typeId);
        int sizeBytes = dataType.getSizeBytes();
        if (sizeBytes == AValue.UNDEFINED_SIZE_BYTES) {
            sizeBytes = value.get(ValueLayout.JAVA_INT_UNALIGNED, BYTE_SIZE);
        }

        return value.asSlice(0, sizeBytes + BYTE_SIZE + (sizeBytes == AValue.UNDEFINED_SIZE_BYTES ? Integer.BYTES : 0));
    }

    public static int findIndexOfKey(DataMemorySegmentValue dataMemorySegmentValue,
                                     IndexMemorySegmentValue indexSegmentValue,
                                     MemorySegment key,
                                     Comparator<MemorySegment> keyComparator) {
        final MemorySegment dataSegment = dataMemorySegmentValue.getMemorySegment();

        int left = 0;
        int right = indexSegmentValue.maxOffsetIndex;

        while (left <= right) {
            final int i = (left + right) >>> 1;
            final long offset = getOffsetByIndex(indexSegmentValue, i);
            final MemorySegment current = MemorySegmentUtils.sliceFirstDbValue(dataSegment.asSlice(offset));

            final int compare = keyComparator.compare(current, key);
            if (compare > 0) {
                right = i - 1;
                continue;
            }

            if (compare < 0) {
                left = i + 1;
                continue;
            }

            return i;
        }

        return -left;
    }

    public static long getOffsetByIndex(IndexMemorySegmentValue indexMemorySegmentValue, int i) {
        return indexMemorySegmentValue.getMemorySegment().get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);
    }

    public static long calculateMemorySegmentsSizeBytes(Collection<MemorySegment> segments) {
        return segments.stream().mapToLong(MemorySegment::byteSize).sum();
    }
}
