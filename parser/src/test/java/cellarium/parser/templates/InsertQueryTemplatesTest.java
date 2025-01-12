package cellarium.parser.templates;

import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

public final class InsertQueryTemplatesTest {
    @Test
    public void testInsertValuesTemplate() {
        final String template = "INSERT INTO table (id, name, age) VALUES (11, Stepan, 32);";

        final Matcher matcher = QueryTemplates.INSERT.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("table", matcher.group(1));
        Assert.assertEquals("id, name, age", matcher.group(2));
        Assert.assertEquals("11, Stepan, 32", matcher.group(3));

        Assert.assertTrue(QueryTemplates.INSERT.matcher(template.toLowerCase()).matches());
    }
}
