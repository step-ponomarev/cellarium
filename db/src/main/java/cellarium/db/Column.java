package cellarium.db;

import cellarium.db.entry.Sizeable;

public interface Column<T, V> extends Sizeable {
    String getName();

    Class<T> getType();

    V getValue();
}
