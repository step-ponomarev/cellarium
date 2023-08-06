package cellarium.db.database;

import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.GetByIdQuery;
import cellarium.db.database.query.UpsertQuery;
import cellarium.db.entry.Entry;

public interface DataBase {
    void createTable(CreateTableQuery query);

    void upsert(UpsertQuery query);

    Entry<Long, ?> getById(GetByIdQuery query);
}
