package cellarium.db;

import cellarium.db.request.CreateTableQuery;
import cellarium.db.request.SelectQuery;
import cellarium.db.request.UpdateQuery;

import java.util.Iterator;

public interface DataBase<V> {
    void createTable(CreateTableQuery query);

    Iterator<TableRow<V, V>> select(SelectQuery<V> query);

    void update(UpdateQuery<V> query);
}
