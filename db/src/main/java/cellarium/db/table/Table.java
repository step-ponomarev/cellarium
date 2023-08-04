package cellarium.db.table;

import cellarium.db.DataBaseColumnType;

import java.util.Map;

public final class Table {
    private final String name;
    private final Map<String, DataBaseColumnType<?>> columns;

    public Table(String name, Map<String, DataBaseColumnType<?>> columnsScheme) {
        this.name = name;
        this.columns = columnsScheme;
    }
}
