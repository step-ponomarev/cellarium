package cellarium.db.database.query;

import cellarium.db.database.types.TypedValue;

import java.util.Map;

public final class UpsertQuery extends Query {
    private final long pk;
    private final Map<String, TypedValue<?>> values;

    public UpsertQuery(String table, long pk, Map<String, TypedValue<?>> values) {
        super(table);
        this.pk = pk;
        this.values = values;
    }

    public long getPk() {
        return pk;
    }

    public Map<String, ?> getValues() {
        return values;
    }
}
