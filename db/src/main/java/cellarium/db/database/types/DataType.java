package cellarium.db.database.types;

public enum DataType {
    LONG(Long.class),
    INTEGER(Integer.class),
    STRING(String.class),
    BOOLEAN(Boolean.class);

    public final Class<?> nativeType;

    DataType(Class<?> nativeType) {
        this.nativeType = nativeType;
    }
}
