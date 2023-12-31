package cellarium.db.database.types;

import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum DataType {

    LONG(Long.class, (byte) 0, Long.BYTES),
    INTEGER(Integer.class, (byte) 1, Integer.BYTES),
    STRING(String.class, (byte) 2, AValue.UNDEFINED_SIZE_BYTES),
    BOOLEAN(Boolean.class, (byte) 3, 1);

    private static final Map<Byte, DataType> ID_TO_MAP = createInitMap();
    public final Class<?> nativeType;
    private final byte id;
    private final int sizeBytes;

    DataType(Class<?> nativeType, byte id, int sizeBytes) {
        this.nativeType = nativeType;
        this.id = id;
        this.sizeBytes = sizeBytes;
    }

    public static DataType getById(byte id) {
        final DataType dataType = ID_TO_MAP.get(id);
        if (dataType == null) {
            throw new IllegalArgumentException("Unsupported type id: " + id);
        }

        return dataType;
    }

    public final byte getId() {
        return id;
    }

    public final int getSizeBytes() {
        return sizeBytes;
    }

    private static Map<Byte, DataType> createInitMap() {
        return Stream.of(DataType.values()).collect(Collectors.toMap(DataType::getId, UnaryOperator.identity()));
    }
}
