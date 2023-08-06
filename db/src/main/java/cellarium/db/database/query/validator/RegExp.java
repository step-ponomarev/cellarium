package cellarium.db.database.query.validator;

import java.util.regex.Pattern;

public final class RegExp {
    public static final int MIN_TABLE_NAME_LEN = 3;
    public static final int MAX_TABLE_NAME_LEN = 128;

    private static final String TABLE_NAME_PATTERN_TEMPLATE = "^[\\p{IsLatin}]{1}[\\p{IsLatin}_\\d]{%d,%d}[\\p{IsLatin}\\d]{1}$";

    public static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
            String.format(TABLE_NAME_PATTERN_TEMPLATE, MIN_TABLE_NAME_LEN - 2, MAX_TABLE_NAME_LEN - 2)
    );

    private RegExp() {}
}
