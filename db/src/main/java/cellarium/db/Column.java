package cellarium.db;

import cellarium.db.entry.WithSizeBytes;

public interface Column<T> extends WithSizeBytes {
    String getName();

    Class<T> getType();

    T getValue();
}
