package cellarium.dao.entry;

public interface Entry<D> {
    D getKey();

    D getValue();
}
