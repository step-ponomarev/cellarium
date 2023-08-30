package cellarium.db.database.validation;

import java.util.regex.Pattern;

final class Regex {
    private static final String NAME_PATTERN_TEMPLATE = "^[a-zA-Z][\\w]{%d,%d}[a-zA-Z\\d]$";

    static Pattern createNamePattern(int minLen, int maxLen) {
        if (minLen < 0 || maxLen < 0 || maxLen < minLen) {
            throw new IllegalArgumentException("Invalid len : [" + minLen + "; " + maxLen + "]");
        }

        return Pattern.compile(
                String.format(NAME_PATTERN_TEMPLATE, minLen - 2, maxLen - 2));
    }

    private Regex() {}
}
