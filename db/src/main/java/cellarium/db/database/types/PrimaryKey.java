package cellarium.db.database.types;

import java.util.Set;

public final class PrimaryKey {
    private static final Set<DataType> VALID_PRIMARY_KEY_TYPES = Set.of(
            DataType.STRING,
            DataType.INTEGER,
            DataType.LONG);

    private final String name;
    private final DataType type;

    public PrimaryKey(String name, DataType type) {
        if (name == null) {
            throw new NullPointerException("Name cannot be null");
        }

        if (type == null) {
            throw new NullPointerException("Type cannot be null");
        }

        this.name = name;

        if (!VALID_PRIMARY_KEY_TYPES.contains(type)) {
            throw new IllegalArgumentException("Unsupported primary key type: " + type);
        }

        this.type = type;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }
}
