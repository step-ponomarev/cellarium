package cellarium.db.serialization.tabllescheme;

import java.util.Map;

import cellarium.db.database.table.TableScheme;
import cellarium.db.serialization.ASerializer;

public final class TableSchemeSerializer extends ASerializer<TableScheme> {
    public static TableSchemeSerializer INSTANCE = new TableSchemeSerializer();

    private TableSchemeSerializer() {
        super(Map.of(0, TableSchemeSerializer_0.INSTANCE));
    }
}
