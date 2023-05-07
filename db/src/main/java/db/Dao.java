package db;

import java.io.Closeable;
import java.io.IOException;
import db.entry.Entry;
import db.store.Store;

public interface Dao<T, E extends Entry<T>> extends Store<T, E>, Closeable {
    default void flush() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }

    default void compact() {
        throw new UnsupportedOperationException("Unsupported method");
    }
}
