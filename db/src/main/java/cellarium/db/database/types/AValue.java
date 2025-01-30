package cellarium.db.database.types;

import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.entry.Sizeable;

public abstract class AValue<V> implements Sizeable, Comparable<AValue<V>> {
    public final static int UNDEFINED_SIZE_BYTES = -1;

    protected final V value;
    protected final DataType dataType;
    protected final long sizeBytes;

    protected AValue(V value, DataType dataType) {
        this(value, dataType, SSTableValueConverter.getSizeOnDisk(dataType, value));
    }

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
    public long getSizeBytesOnDisk() {
        return sizeBytes;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || obj.hashCode() != this.hashCode()) {
            return false;
        }

        return (obj instanceof AValue<?> val) && equals(this.dataType, val, this);
    }

    static boolean equals(DataType dataType, AValue<?> val1, AValue<?> val2) {
        if (val1.dataType != val2.dataType) {
            return false;
        }

        return switch (dataType) {
            case INTEGER -> ((IntegerValue) val1).compareTo((IntegerValue) val2) == 0;
            case LONG -> ((LongValue) val1).compareTo((LongValue) val2) == 0;
            case STRING -> ((StringValue) val1).compareTo((StringValue) val2) == 0;
            case BOOLEAN -> ((BooleanValue) val1).compareTo((BooleanValue) val2) == 0;
        };
    }

    @Override
    public abstract int hashCode();
}
