package cellarium.db;

import java.io.Closeable;
import java.io.IOException;
import cellarium.db.store.Store;
import cellarium.db.entry.Entry;

public interface Dao<T, E extends Entry<T>> extends Store<T, E>, Closeable {
    default void flush() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }

    default void compact() {
        throw new UnsupportedOperationException("Unsupported method");
    }
}
