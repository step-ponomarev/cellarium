package cellarium.db.entry;

public interface Entry<PK, V> {
    PK getPK();

    V getValue();
}
