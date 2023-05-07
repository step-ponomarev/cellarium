package db.entry;

public interface Entry<D> {
    D getKey();

    D getValue();
}
