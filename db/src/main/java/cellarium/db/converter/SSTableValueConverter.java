package cellarium.db.converter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;

public final class SSTableValueConverter implements Converter<AValue<?>, MemorySegment> {
    public static final SSTableValueConverter INSTANCE = new SSTableValueConverter();

    private static final byte BYTE_SIZE = 1;

    @Override
    public MemorySegment convert(AValue<?> value) {
        final DataType dataType = value.getDataType();
        final boolean undefiledValueSize = dataType.getSizeBytes() == AValue.UNDEFINED_SIZE_BYTES;

        // if string for example will write size
        final MemorySegment segment = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                // to understand string typing see java.lang.foreign.SegmentAllocator.allocateUtf8String
                BYTE_SIZE + value.getSizeBytes() + (undefiledValueSize ? Integer.BYTES : 0) + (dataType == DataType.STRING ? 1 : 0)
        );

        int offset = 0;
        segment.asSlice(offset, BYTE_SIZE).set(ValueLayout.JAVA_BYTE, 0, dataType.getId());
        offset += BYTE_SIZE;

        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);
        final MemorySegment memorySegment = converter.convert(value.getValue());

        if (undefiledValueSize) {
            segment.asSlice(offset, Integer.BYTES).set(ValueLayout.JAVA_INT_UNALIGNED, 0, (int) memorySegment.byteSize());
            offset += Integer.BYTES;
        }

        segment.asSlice(offset, memorySegment.byteSize()).copyFrom(memorySegment);

        return segment;
    }

    @Override
    public AValue<?> convertBack(MemorySegment value) {
        final byte typeId = value.get(ValueLayout.JAVA_BYTE, 0);
        final DataType dataType = DataType.getById(typeId);
        int sizeBytes = dataType.getSizeBytes();
        if (sizeBytes != AValue.UNDEFINED_SIZE_BYTES) {
            return MemorySegmentUtils.toValue(
                    dataType,
                    value.asSlice(BYTE_SIZE, sizeBytes)
            );
        }
        sizeBytes = value.get(ValueLayout.JAVA_INT_UNALIGNED, BYTE_SIZE);

        return MemorySegmentUtils.toValue(
                dataType,
                value.asSlice(BYTE_SIZE + Integer.BYTES, sizeBytes)
        );
    }
}
