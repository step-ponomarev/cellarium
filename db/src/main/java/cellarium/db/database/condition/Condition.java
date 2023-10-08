package cellarium.db.database.condition;

import java.util.Map;

import cellarium.db.database.types.AValue;

public interface Condition {
    public boolean metches(Map<String, AValue<?>> row);
}
