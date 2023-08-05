package cellarium.db.database.query;

import cellarium.db.database.types.TypedValue;

import java.util.Map;

public final class UpdateQuery extends Query {
    private final Map<String, TypedValue<?>> newValues;

    public UpdateQuery(String table, Map<String, TypedValue<?>> newValues, Map<String, TypedValue<?>> where) {
        super(table);
        this.newValues = newValues;
    }

    public Map<String, TypedValue<?>> getNewValues() {
        return newValues;
    }
}
