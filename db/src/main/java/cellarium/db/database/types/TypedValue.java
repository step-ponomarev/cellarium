package cellarium.db.database.types;

import cellarium.db.entry.Sizeable;

public final class TypedValue<V> implements Sizeable {
    private final DataType dataType;
    private final V value;
    private final long sizeBytes;

    public TypedValue(V value) {
        this.dataType = DataType.typeOf(value);
        if (this.dataType == null) {
            throw new IllegalArgumentException("Unsupported type " + value.getClass().toString());
        }
        this.value = value;
        this.sizeBytes = DataType.sizeOf(value);
    }

    public DataType getDataType() {
        return dataType;
    }

    public V getValue() {
        return value;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }
}
