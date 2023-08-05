package cellarium.db.database.query.validator;

import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.types.DataType;

import java.util.Map;

public final class CreateTableQueryValidator implements QueryValidator<CreateTableQuery> {
    public static final CreateTableQueryValidator INSTANCE = new CreateTableQueryValidator();

    private CreateTableQueryValidator() {}

    @Override
    public void validate(CreateTableQuery query) {
        TableNameValidator.INSTANCE.validate(query);

        final Map<String, DataType> columnScheme = query.getColumnScheme();
        if (columnScheme == null) {
            throw new NullPointerException("Table scheme is null");
        }

        if (columnScheme.isEmpty()) {
            throw new IllegalArgumentException("Table scheme is empty");
        }
    }
}
