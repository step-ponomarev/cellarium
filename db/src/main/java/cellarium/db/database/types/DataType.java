package cellarium.db.database.types;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public enum DataType {
    LONG(Long.class),
    INTEGER(Integer.class),
    STRING(String.class),
    BOOLEAN(Boolean.class);

    private static final Map<Class<?>, DataType> SUPPORTED_DATA_TYPES = new HashMap<>();
    static {
        for (DataType type : DataType.values()) {
            SUPPORTED_DATA_TYPES.put(type.nativeType, type);
        }
    }

    public final Class<?> nativeType;

    DataType(Class<?> nativeType) {
        this.nativeType = nativeType;
    }

    static <V> long sizeOf(V value) {
        final DataType type = typeOf(value);
        if (type == null) {
            throw new IllegalStateException("Unsupported type " + value.getClass());
        }

        return switch (type) {
            case LONG -> Long.BYTES;
            case INTEGER -> Integer.BYTES;
            case BOOLEAN -> 1;
            case STRING -> ((String) value).getBytes(StandardCharsets.UTF_8).length;
        };
    }

    static <V> DataType typeOf(V value) {
        return SUPPORTED_DATA_TYPES.get(value.getClass());
    }
}
