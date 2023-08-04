package cellarium.db;

import cellarium.db.entry.Sizeable;

public interface TableColumn<V> extends Sizeable {
    DataBaseColumnType<V> getType();

    V getValue();
}
