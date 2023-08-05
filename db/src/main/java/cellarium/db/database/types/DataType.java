package cellarium.db.database.types;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public enum DataType {
    LONG(Long.class),
    INTEGER(Integer.class),
    STRING(String.class),
    BOOLEAN(Boolean.class);

    private static final Map<Class<?>, DataType> DATA_TYPES = new HashMap<>();

    static {
        for (DataType type : DataType.values()) {
            DATA_TYPES.put(type.nativeType, type);
        }
    }

    DataType(Class<?> nativeType) {
        this.nativeType = nativeType;
    }

    public static <V> long sizeOf(V value) {
        final DataType type = typeOf(value);
        if (type == null) {
            throw new IllegalStateException("Unsupported type " + value.getClass());
        }

        return
                switch (type) {
                    case LONG -> Long.BYTES;
                    case INTEGER -> Integer.BYTES;
                    case BOOLEAN -> 1;
                    case STRING -> ((String) value).getBytes(StandardCharsets.UTF_8).length;
                };
    }

    public <V> boolean isTypeOf(V value) {
        return nativeType.equals(value.getClass());
    }

    public static <V> DataType typeOf(V value) {
        return DATA_TYPES.get(value.getClass());
    }

    public final Class<?> nativeType;
}
