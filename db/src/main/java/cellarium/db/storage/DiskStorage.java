package cellarium.db.storage;

import java.io.Closeable;
import java.io.IOException;

import cellarium.db.entry.Entry;

public interface DiskStorage<T, E extends Entry<T, ?>> extends Storage<T, E>, Closeable {
    default void flush() throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }

    default void compact() {
        throw new UnsupportedOperationException("Unsupported method");
    }
}
