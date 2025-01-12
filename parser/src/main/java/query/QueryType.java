package query;

import java.util.regex.Pattern;

import cellarium.parser.templates.QueryTemplates;

public enum QueryType {
    SELECT(QueryTemplates.SELECT),
    SELECT_WHERE(QueryTemplates.SELECT_WHERE),
    INSERT(QueryTemplates.INSERT),
    DELETE(QueryTemplates.DELETE),
    DROP_TABLE(QueryTemplates.DROP_TABLE);

    public final Pattern pattern;

    QueryType(Pattern pattern) {
        this.pattern = pattern;
    }
}
