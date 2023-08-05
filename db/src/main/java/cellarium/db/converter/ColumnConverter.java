package cellarium.db.converter;

public interface ColumnConverter<T, R> {
    R convert(T value);

    T convertBack(R value);
}
