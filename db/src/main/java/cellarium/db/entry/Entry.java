package cellarium.db.entry;

public interface Entry<D, V> {
    D getKey();

    V getValue();
}
