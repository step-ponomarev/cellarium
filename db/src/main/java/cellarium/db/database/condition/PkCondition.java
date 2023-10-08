package cellarium.db.database.condition;

import java.util.Map;

import cellarium.db.database.types.AValue;

public final class PkCondition<PK> implements Condition {
    private final String pk;
    private final AValue<PK> value;

    public PkCondition(String pk, AValue<PK> value) {
        this.pk = pk;
        this.value = value;
    }

    @Override
    public boolean metches(Map<String, AValue<?>> row) {
        final AValue<PK> pkValue = (AValue<PK>) row.get(pk);
        if (pkValue == null) {
            throw new IllegalStateException("PK cannot be null");
        }

        return value.compareTo(pkValue) == 0;
    }
}
