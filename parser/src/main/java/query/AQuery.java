package query;

public class AQuery {
    private final String tableName;
    private final QueryType queryType;

    protected AQuery(QueryType queryType, String tableName) {
        if (queryType == null || tableName == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }
        this.queryType = queryType;
        this.tableName = tableName;
    }
}
