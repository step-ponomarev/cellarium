package query;

public final class SelectQuery extends AQuery {
    protected SelectQuery(QueryBuilder<SelectQuery> builder) {
        super(QueryType.SELECT, builder.tableName);
    }

    public static QueryBuilder<SelectQuery> builder() {
        return new QueryBuilder<>() {
            @Override
            protected SelectQuery build() {
                return new SelectQuery(this);
            }
        };
    }
}
