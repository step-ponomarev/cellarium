package cellarium.db.regex;

import cellarium.db.database.Regex;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public final class NameRegexTest {
    private static final int MIN_LEN = 10;
    private static final int MAX_LEN = 22;

    private static final Pattern NAME_PATTERN = Regex.createNamePattern(MIN_LEN, MAX_LEN);

    @Test
    public void testEmptySting() {
        Assert.assertFalse(
                NAME_PATTERN.matcher("").matches());
    }

    @Test
    public void testBlankSting() {
        Assert.assertFalse(
                NAME_PATTERN.matcher(" ").matches());
    }

    @Test
    public void testStingWithSpaces() {
        Assert.assertFalse(
                NAME_PATTERN.matcher("table name").matches());
    }

    @Test
    public void testMaxLenString() {
        String string = createValidTavleNameString("s", MAX_LEN);

        Assert.assertTrue(
                NAME_PATTERN.matcher(string).matches());
    }

    @Test
    public void testBiggerThanMaxLenString() {
        String string = createValidTavleNameString("s", MAX_LEN + 1);

        Assert.assertFalse(
                NAME_PATTERN.matcher(string).matches());
    }

    @Test
    public void testMinLenString() {
        String string = createValidTavleNameString("s", MIN_LEN);

        Assert.assertTrue(
                NAME_PATTERN.matcher(string).matches());
    }

    @Test
    public void testLessThanMinLenString() {
        String string = createValidTavleNameString("s", MIN_LEN - 1);

        Assert.assertFalse(
                NAME_PATTERN.matcher(string).matches());
    }

    @Test
    public void testBeginWithUnderscoreSting() {
        Assert.assertFalse(
                NAME_PATTERN.matcher(createValidTavleNameString("_", MIN_LEN)).matches());
    }

    @Test
    public void testEndWithUnderscoreSting() {
        Assert.assertFalse(
                NAME_PATTERN.matcher(createValidTavleNameString("s", MIN_LEN - 1) + "_")
                        .matches());
    }

    @Test
    public void testBeginWithDigitSting() {
        Assert.assertFalse(
                NAME_PATTERN.matcher(createValidTavleNameString("0", MIN_LEN)).matches());
    }

    @Test
    public void testCreatedStringLength() {
        final int len = 128;
        final String string = createValidTavleNameString("s", len);

        Assert.assertEquals(len, string.length());
    }

    @Test
    public void testCreatedStingCorrectTableName() {
        final String validTableName = createValidTavleNameString("s", 12);

        Assert.assertTrue(
                NAME_PATTERN.matcher(validTableName).matches());
    }

    private static String createValidTavleNameString(String startSymbol, int len) {
        StringBuilder str = new StringBuilder(startSymbol);

        for (int i = 0; i < len - 1; i++) {
            if ((i & 1) == 1) {
                str.append(2);
            } else {
                str.append("s");
            }
        }

        return str.toString();
    }
}
