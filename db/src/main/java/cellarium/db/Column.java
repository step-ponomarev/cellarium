package cellarium.db;

import cellarium.db.entry.Sizeable;

public interface Column<T> extends Sizeable {
    String getName();

    Class<T> getType();

    T getValue();
}
