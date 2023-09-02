package cellarium.db.database.condition;

import cellarium.db.database.types.AValue;

public final class Condition {
    public final AValue<?> from;
    public final AValue<?> to;

    public Condition(AValue<?> from, AValue<?> to) {
        this.from = from;
        this.to = to;
    }
}
