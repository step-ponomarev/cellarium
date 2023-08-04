package cellarium.db.request;

import cellarium.db.TableColumn;

import java.util.Map;

public class WhereQuery<V> extends Query {
    private final Map<String, TableColumn<V>> where;

    public WhereQuery(String table, Map<String, TableColumn<V>> where) {
        super(table);
        this.where = where;
    }

    public Map<String, TableColumn<V>> getWhere() {
        return where;
    }
}
