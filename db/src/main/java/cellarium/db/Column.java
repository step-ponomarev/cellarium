package cellarium.db;

public interface Column<T> {
    String getName();

    Class<T> getType();

    <T> T getValue();

    long getSizeBytes();
}
