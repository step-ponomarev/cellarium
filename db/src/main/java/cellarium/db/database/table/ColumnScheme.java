package cellarium.db.database.table;

import cellarium.db.database.types.DataType;

public final class ColumnScheme {
    private final String name;
    private final DataType type;

    public ColumnScheme(String name, DataType type) {
        if (name == null) {
            throw new NullPointerException("Name cannot be null");
        }

        if (type == null) {
            throw new NullPointerException("Type cannot be null");
        }

        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }
}
