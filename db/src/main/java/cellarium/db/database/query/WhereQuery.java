package cellarium.db.database.query;

import cellarium.db.table.TableColumn;

import java.util.Map;

public class WhereQuery extends Query {
    private final Map<String, TableColumn<?>> where;

    public WhereQuery(String table, Map<String, TableColumn<?>> where) {
        super(table);
        this.where = where;
    }

    public Map<String, TableColumn<?>> getWhere() {
        return where;
    }
}
