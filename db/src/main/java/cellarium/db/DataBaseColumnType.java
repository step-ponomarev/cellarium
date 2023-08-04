package cellarium.db;

public interface DataBaseColumnType<T> {
    Class<T> getNativeType();
}

