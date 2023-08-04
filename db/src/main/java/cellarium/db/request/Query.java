package cellarium.db.request;

public class Query {
    private final String table;

    public Query(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }
}
