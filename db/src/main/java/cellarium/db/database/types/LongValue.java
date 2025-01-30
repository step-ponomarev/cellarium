package cellarium.db.database.types;

public final class LongValue extends AValue<Long> {
    private LongValue(long value) {
        super(value, DataType.LONG);
    }

    public static LongValue of(long value) {
        return new LongValue(value);
    }

    @Override
    public int compareTo(AValue<Long> o) {
        return Long.compare(this.value, o.value);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }
}
