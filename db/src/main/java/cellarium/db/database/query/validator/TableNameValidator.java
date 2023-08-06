package cellarium.db.database.query.validator;

import cellarium.db.database.query.Query;


final class TableNameValidator implements QueryValidator<Query> {
    public static final TableNameValidator INSTANCE = new TableNameValidator();

    private TableNameValidator() {}

    //TODO: Написать регулярку нормальную для провервки
    @Override
    public void validate(Query query) {
        final String tableName = query.getTableName();

        if (tableName == null) {
            throw new NullPointerException("Name is null");
        }

        if (tableName.isBlank()) {
            throw new IllegalArgumentException("Table name is blank");
        }

        if (tableName.trim().contains("\s")) {
            throw new IllegalArgumentException("Table name cannot consist of spaces");
        }
    }
}
