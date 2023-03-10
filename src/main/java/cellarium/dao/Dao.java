package cellarium.dao;

import java.io.Closeable;
import java.io.IOException;
import cellarium.entry.Entry;
import cellarium.store.Store;

public interface Dao<T, E extends Entry<T>> extends Store<T, E>, Closeable {
    default void flush() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }

    default void compact() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }
}
