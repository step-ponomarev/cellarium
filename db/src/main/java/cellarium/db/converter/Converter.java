package cellarium.db.converter;

public interface Converter<T, R> {
    R convert(T value);

    T convertBack(R value);
}
