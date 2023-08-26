package cellarium.db.database.condition;

import cellarium.db.database.types.AValue;

import java.util.Map;

public final class Condition {
    private final Map<String, ValueCandition> conditions;

    public Condition(Map<String, ValueCandition> conditions) {
        this.conditions = conditions;
    }

    public static final class ValueCandition {
        public final AValue value;

        public ValueCandition(AValue value) {
            this.value = value;
        }
    }

    public Map<String, ValueCandition> getConditions() {
        return conditions;
    }
}
