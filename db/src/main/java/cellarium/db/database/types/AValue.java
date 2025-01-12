package cellarium.db.database.types;

import cellarium.db.entry.Sizeable;

public abstract class AValue<V> implements Sizeable, Comparable<AValue<V>> {
    public final static int UNDEFINED_SIZE_BYTES = -1;

    protected final V value;
    protected final DataType dataType;
    protected final long sizeBytes;

    protected AValue(V value, DataType dataType, long sizeBytes) {
        this.value = value;
        this.dataType = dataType;
        this.sizeBytes = sizeBytes;
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
