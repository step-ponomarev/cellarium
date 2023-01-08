package cellarium.store;

import java.io.IOException;
import java.util.Iterator;
import cellarium.entry.Entry;

public interface Store<T, E extends Entry<T>> {
    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     *
     * @param from lower bound of range (inclusive)
     * @param to   upper bound of range (exclusive)
     * @return entries [from;to)
     */
    Iterator<E> get(T from, T to) throws IOException;

    E get(T key) throws IOException;

    default void upsert(E entry) {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
