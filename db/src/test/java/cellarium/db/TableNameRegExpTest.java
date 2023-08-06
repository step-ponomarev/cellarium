package cellarium.db;

import cellarium.db.database.query.validator.RegExp;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public final class TableNameRegExpTest {
    private static final Pattern TABLE_NAME_PATTERN = RegExp.TABLE_NAME_PATTERN;

    @Test
    public void testEmptySting() {
        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher("").matches()
        );
    }

    @Test
    public void testBlankSting() {
        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher(" ").matches()
        );
    }

    @Test
    public void testStingWithSpaces() {
        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher("table name").matches()
        );
    }

    @Test
    public void testMaxLenString() {
        String string = createValidTavleNameString("s", RegExp.MAX_TABLE_NAME_LEN);

        Assert.assertTrue(
                TABLE_NAME_PATTERN.matcher(string).matches()
        );
    }

    @Test
    public void testBiggerThanMaxLenString() {
        String string = createValidTavleNameString("s", RegExp.MAX_TABLE_NAME_LEN + 1);

        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher(string).matches()
        );
    }

    @Test
    public void testMinLenString() {
        String string = createValidTavleNameString("s", RegExp.MIN_TABLE_NAME_LEN);

        Assert.assertTrue(
                TABLE_NAME_PATTERN.matcher(string).matches()
        );
    }

    @Test
    public void testLessThanMinLenString() {
        String string = createValidTavleNameString("s", RegExp.MIN_TABLE_NAME_LEN - 1);

        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher(string).matches()
        );
    }

    @Test
    public void testBeginWithUnderscoreSting() {
        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher(createValidTavleNameString("_", RegExp.MIN_TABLE_NAME_LEN)).matches()
        );
    }

    @Test
    public void testEndWithUnderscoreSting() {
        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher(createValidTavleNameString("s", RegExp.MIN_TABLE_NAME_LEN - 1) + "_").matches()
        );
    }

    @Test
    public void testBeginWithDigitSting() {
        Assert.assertFalse(
                TABLE_NAME_PATTERN.matcher(createValidTavleNameString("0", RegExp.MIN_TABLE_NAME_LEN)).matches()
        );
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
                TABLE_NAME_PATTERN.matcher(validTableName).matches()
        );
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
