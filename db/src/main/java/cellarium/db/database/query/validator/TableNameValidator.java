package cellarium.db.database.query.validator;

import cellarium.db.database.query.Query;


public final class TableNameValidator implements QueryValidator<Query> {
    public static final TableNameValidator INSTANCE = new TableNameValidator();

    private TableNameValidator() {}

    //TODO: Написать регулярку нормальную для провервки
    @Override
    public void validate(Query query) {
        final String tableName = query.getTableName();

        if (tableName == null) {
            throw new NullPointerException("Name is null");
        }

        if (!RegExp.TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name " + tableName);
        }
    }
}
