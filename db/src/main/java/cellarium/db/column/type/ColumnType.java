package cellarium.db.column.type;

public enum ColumnType {
    STRING((short) 0),
    INTEGER((short) 1),
    UNSIGNED_INTEGER((short) 2),
    BOOLEAN((short) 3);

    private short id;

    ColumnType(short id) {
        this.id = id;
    }
}
