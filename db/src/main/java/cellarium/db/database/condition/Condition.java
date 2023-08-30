package cellarium.db.database.condition;

import cellarium.db.database.types.AValue;

public final class Condition {
    private AValue<?> from;
    private AValue<?> to;

    public Condition(AValue<?> from, AValue<?> to) {
        this.from = from;
        this.to = to;
    }
}
