package cellarium.db.database.query;

public final class GetByIdQuery extends Query {
    private final long id;

    public GetByIdQuery(String tableName, long id) {
        super(tableName);
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
