package cellarium.db.table;

import cellarium.db.entry.Sizeable;

public interface TableColumn<T> extends Sizeable {
    DataBaseColumnType<T> getType();

    T getValue();
}
