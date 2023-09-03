package cellarium.db.database.types;

import java.nio.charset.StandardCharsets;

public final class StringValue extends AValue<String> {
    public StringValue(String value, DataType dataType) {
        super(value, dataType, getSizeBytes(value));
    }

    @Override
    public int compareTo(AValue<String> o) {
        return value.compareTo(o.value);
    }

    private static long getSizeBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8).length;
    }
}
