package cellarium.db.database.types;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    ID(Long.class),
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

    public <V> boolean isTypeOf(V value) {
        return nativeType.equals(value.getClass());
    }

    public static <V> DataType typeOf(V value) {
        return DATA_TYPES.get(value.getClass());
    }

    public final Class<?> nativeType;
}
