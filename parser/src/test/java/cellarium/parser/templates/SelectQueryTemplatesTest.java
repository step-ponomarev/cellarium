package cellarium.parser.templates;

import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

public final class SelectQueryTemplatesTest {
    @Test
    public void testSelectAllTemplate() {
        final String template = "SELECT * FROM table;";

        final Matcher matcher = QueryTemplates.SELECT.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("*", matcher.group(1));
        Assert.assertEquals("table", matcher.group(2));

        Assert.assertTrue(QueryTemplates.SELECT.matcher(template.toLowerCase()).matches());
    }

    @Test
    public void testSelectValuesTemplate() {
        final String template = "SELECT VALUES(id, name, age) FROM table;";

        final Matcher matcher = QueryTemplates.SELECT.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("VALUES(id, name, age)", matcher.group(1));
        Assert.assertEquals("table", matcher.group(2));

        Assert.assertTrue(QueryTemplates.SELECT.matcher(template.toLowerCase()).matches());
    }

    @Test
    public void testSelectWhereAllTemplate() {
        final String template = "SELECT VALUES(id, name, age) FROM table WHERE id=12;";

        final Matcher matcher = QueryTemplates.SELECT_WHERE.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("VALUES(id, name, age)", matcher.group(1));
        Assert.assertEquals("table", matcher.group(2));
        Assert.assertEquals("id", matcher.group(3));
        Assert.assertEquals("12", matcher.group(4));

        Assert.assertTrue(QueryTemplates.SELECT_WHERE.matcher(template.toLowerCase()).matches());
    }

    @Test
    public void testSelectWhereValuesTemplate() {
        final String template = "SELECT * FROM table WHERE id=12;";

        final Matcher matcher = QueryTemplates.SELECT_WHERE.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("*", matcher.group(1));
        Assert.assertEquals("table", matcher.group(2));
        Assert.assertEquals("id", matcher.group(3));
        Assert.assertEquals("12", matcher.group(4));

        Assert.assertTrue(QueryTemplates.SELECT_WHERE.matcher(template.toLowerCase()).matches());
    }
}
