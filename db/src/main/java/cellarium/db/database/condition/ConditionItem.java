package cellarium.db.database.condition;

import cellarium.db.database.types.AValue;

public final class ConditionItem {
    public final ComparisonOperator operator;
    public final AValue<?> value;

    public ConditionItem(ComparisonOperator operator, AValue<?> value) {
        this.operator = operator;
        this.value = value;
    }
}