package query;

public abstract class QueryBuilder<T extends AQuery> {
    public String tableName;

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    protected abstract T build();
}
