package cellarium.parser.templates;

import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

public final class DeleteQueryTemplatesTest {
    @Test
    public void testSelectValuesTemplate() {
        final String template = "DELETE FROM table WHERE id=212;";

        final Matcher matcher = QueryTemplates.DELETE.matcher(template);
        Assert.assertTrue(matcher.matches());

        Assert.assertEquals("table", matcher.group(1));
        Assert.assertEquals("id", matcher.group(2));
        Assert.assertEquals("212", matcher.group(3));

        Assert.assertTrue(QueryTemplates.DELETE.matcher(template.toLowerCase()).matches());
    }
}
