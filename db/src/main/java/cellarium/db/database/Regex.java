package cellarium.db.database;

import java.util.regex.Pattern;

public final class Regex {
    private static final String NAME_PATTERN_TEMPLATE = "^[a-zA-Z][\\w]{%d,%d}[a-zA-Z\\d]$";
    public static final Pattern TABLE_NAME_PATTERN = createNamePattern(3, 128);
    public static final Pattern COLUMN_NAME_PATTERN = createNamePattern(3, 30);

    public static Pattern createNamePattern(int minLen, int maxLen) {
        return Pattern.compile(
                String.format(NAME_PATTERN_TEMPLATE, minLen - 2, maxLen - 2));
    }

    private Regex() {}
}
