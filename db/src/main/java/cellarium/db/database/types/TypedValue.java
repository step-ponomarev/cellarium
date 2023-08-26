package cellarium.db.database.types;

import cellarium.db.entry.Sizeable;

public final class TypedValue implements Sizeable {
    private final Object value;
    private final DataType dataType;
    private final long sizeBytes;

    public TypedValue(Object value) {
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

    public Object getValue() {
        return value;
    }

    @Override
    public long getSizeBytes() {
        return sizeBytes;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return value.equals(obj);
    }
}
