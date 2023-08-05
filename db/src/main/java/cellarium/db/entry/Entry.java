package cellarium.db.entry;

public interface Entry<K, V> extends WithKey<K> {
    V getValue();
}
