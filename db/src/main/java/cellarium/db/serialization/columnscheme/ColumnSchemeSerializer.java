package cellarium.db.serialization.columnscheme;

import java.util.Map;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.serialization.ASerializer;

public final class ColumnSchemeSerializer extends ASerializer<ColumnScheme> {
    public static ColumnSchemeSerializer INSTANCE = new ColumnSchemeSerializer();

    private ColumnSchemeSerializer() {
        super(Map.of(0, ColumnSchemeSerializer_0.INSTANCE));
    }
}
