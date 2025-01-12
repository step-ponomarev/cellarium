package cellarium.parser.templates;

import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

public final class DropTableQueryTemplatesTest {
    @Test
    public void dropTableTemplateTest() {
        final String template = "DROP TABLE table;";

        final Matcher matcher = QueryTemplates.DROP_TABLE.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("table", matcher.group(1));

        Assert.assertTrue(QueryTemplates.DROP_TABLE.matcher(template.toLowerCase()).matches());
    }
}
