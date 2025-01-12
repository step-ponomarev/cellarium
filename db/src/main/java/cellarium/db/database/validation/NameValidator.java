package cellarium.db.database.validation;

import java.util.regex.Pattern;

public final class NameValidator {
    public static final int MAX_NAME_LEN = 128;
    public static final int MAX_COLUMN_LEN = 30;

    private static final Pattern TABLE_NAME_PATTERN = Regex.createNamePattern(3, MAX_NAME_LEN);
    private static final Pattern COLUMN_NAME_PATTERN = Regex.createNamePattern(2, MAX_COLUMN_LEN);

    private NameValidator() {}

    public static void validateColumnNames(Iterable<String> names) {
        if (names == null) {
            return;
        }

        for (String name : names) {
            validateColumnName(name);
        }
    }

    public static void validateColumnName(String name) {
        if (!COLUMN_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid column name: " + name);
        }
    }

    public static void validateTableName(String tableName) {
        if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }
}
