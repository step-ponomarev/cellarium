package cellarium.db.table;

import java.util.Map;
import java.util.Set;
import cellarium.db.column.Column;

public final class Table {
    private final Map<String, Column> columns;
    private final String pkColumnName;

//    public Table(Set<Column> columns, String pkColumnName) {
//        this.columns = columns;
//        this.pkColumnName = pkColumnName;
//    }
}
