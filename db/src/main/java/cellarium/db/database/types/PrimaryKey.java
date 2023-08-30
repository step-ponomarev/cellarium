package cellarium.db.database.types;

public final class PrimaryKey {
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
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }
}
