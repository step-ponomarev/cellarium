package cellarium.db.database;

import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.SelectQuery;
import cellarium.db.database.query.UpdateQuery;
import cellarium.db.database.query.UpsertQuery;
import cellarium.db.table.TableRow;

import java.util.Iterator;

public interface DataBase {
    void createTable(CreateTableQuery query);

    void upsert(UpsertQuery query);

    Iterator<TableRow<?, ?>> select(SelectQuery query);

    void update(UpdateQuery query);
}
