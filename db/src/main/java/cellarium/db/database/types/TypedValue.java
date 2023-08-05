package cellarium.db.database.types;

public final class TypedValue<V> {
    private final DataType dataType;
    private final V value;

    public TypedValue(V value) {
        this.dataType = DataType.typeOf(value);
        if (this.dataType == null) {
            throw new IllegalArgumentException("Unsupported type " + value.getClass().toString());
        }
        this.value = value;
    }

    public DataType getDataType() {
        return dataType;
    }

    public V getValue() {
        return value;
    }
}
