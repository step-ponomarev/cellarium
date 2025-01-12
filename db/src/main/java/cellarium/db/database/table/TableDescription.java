package cellarium.db.database.table;

public final class TableDescription {
    public final String tableName;
    public final TableScheme tableScheme;

    public TableDescription(String tableName, TableScheme tableScheme) {
        this.tableName = tableName;
        this.tableScheme = tableScheme;
    }
}
