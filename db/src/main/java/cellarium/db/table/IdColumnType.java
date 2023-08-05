package cellarium.db.table;

public final class IdColumnType implements DataBaseColumnType<Long> {
    public static final IdColumnType INSTANCE = new IdColumnType();

    private IdColumnType() {}

    @Override
    public Class<Long> getNativeType() {
        return Long.class;
    }
}
