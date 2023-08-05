package cellarium.db.database.query;

public class Query {
    private final String tableName;

    public Query(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
