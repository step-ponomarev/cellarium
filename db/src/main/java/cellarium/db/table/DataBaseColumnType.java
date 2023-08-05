package cellarium.db.table;

public interface DataBaseColumnType<T> {
    Class<T> getNativeType();
}

