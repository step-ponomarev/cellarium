package cellarium.db.storage;

import cellarium.db.entry.Entry;

import java.util.Iterator;

public interface Storage<K, E extends Entry<K, ?>> {
    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     *
     * @param from lower bound of range (inclusive)
     * @param to   upper bound of range (exclusive)
     * @return entries [from;to)
     */
    Iterator<E> get(K from, K to);

    E get(K key);

    default void put(E entry) {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
