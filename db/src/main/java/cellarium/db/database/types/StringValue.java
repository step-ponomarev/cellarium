package cellarium.db.database.types;

public final class StringValue extends AValue<String> {
    private StringValue(String value) {
        super(value, DataType.STRING);
    }

    public static StringValue of(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        return new StringValue(value);
    }

    @Override
    public int compareTo(AValue<String> o) {
        return value.compareTo(o.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
