package cellarium.dao;

import java.io.Closeable;
import java.io.IOException;
import cellarium.store.Store;
import cellarium.entry.Entry;

public interface Dao<T, E extends Entry<T>> extends Store<T, E>, Closeable {

    default void flush() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }

    default void compact() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }
}
