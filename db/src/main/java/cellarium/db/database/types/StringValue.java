package cellarium.db.database.types;

import java.nio.charset.StandardCharsets;

public final class StringValue extends AValue<String> {
    private StringValue(String value) {
        super(value, DataType.STRING, getSizeBytes(value));
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

    private static long getSizeBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8).length;
    }
}
