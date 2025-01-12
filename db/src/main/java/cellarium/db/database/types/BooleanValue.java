package cellarium.db.database.types;

public final class BooleanValue extends AValue<Boolean> {
    private BooleanValue(boolean value) {
        super(value, DataType.BOOLEAN, 1);
    }

    public static BooleanValue of(boolean value) {
        return new BooleanValue(value);
    }

    @Override
    public int compareTo(AValue<Boolean> o) {
        return Boolean.compare(this.value, o.value);
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(value);
    }
}
