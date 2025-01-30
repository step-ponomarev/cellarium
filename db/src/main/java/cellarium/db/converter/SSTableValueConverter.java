package cellarium.db.converter;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;

public final class SSTableValueConverter implements Converter<AValue<?>, MemorySegment> {
    public static final SSTableValueConverter INSTANCE = new SSTableValueConverter();

    public static final class ValueWithSize {
        private final AValue<?> value;
        private final long valueSizeOnDisk;

        public ValueWithSize(AValue<?> value, long valueSizeOnDisk) {
            this.value = value;
            this.valueSizeOnDisk = valueSizeOnDisk;
        }

        public long getValueSizeOnDisk() {
            return valueSizeOnDisk;
        }

        public AValue<?> getValue() {
            return value;
        }
    }

    public static long getSizeOnDisk(DataType type, Object value) {
        if (type.getSizeBytes() != AValue.UNDEFINED_SIZE_BYTES) {
            return Byte.BYTES + type.getSizeBytes();
        }

        if (type != DataType.STRING) {
            throw new UnsupportedOperationException(STR."Unsupported type: \{type}");
        }

        final String strValue = (String) value;
        // to understand string typing see java.lang.foreign.SegmentAllocator.allocateUtf8String (+1)
        return MemorySegmentUtils.BYTE_SIZE + strValue.getBytes(StandardCharsets.UTF_8).length + Integer.BYTES + 1;
    }

    @Override
    public MemorySegment convert(AValue<?> value) {
        final DataType dataType = value.getDataType();

        // if string for example will write size
        final MemorySegment segment = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                getSizeOnDisk(value.getDataType(), value.getValue())
        );

        int offset = 0;
        segment.set(ValueLayout.JAVA_BYTE, offset, dataType.getId());
        offset += MemorySegmentUtils.BYTE_SIZE;

        final Converter<Object, MemorySegment> converter = ConverterFactory.getConverter(dataType);
        final MemorySegment memorySegment = converter.convert(value.getValue());

        if (dataType == DataType.STRING) {
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, offset, (int) memorySegment.byteSize());
            offset += Integer.BYTES;
        }

        MemorySegment.copy(memorySegment, 0, segment, offset, memorySegment.byteSize());

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
                    value.asSlice(MemorySegmentUtils.BYTE_SIZE, sizeBytes)
            );
        }

        sizeBytes = value.get(ValueLayout.JAVA_INT_UNALIGNED, MemorySegmentUtils.BYTE_SIZE);

        return MemorySegmentUtils.toValue(
                dataType,
                value.asSlice(MemorySegmentUtils.BYTE_SIZE + Integer.BYTES, sizeBytes)
        );
    }
}
