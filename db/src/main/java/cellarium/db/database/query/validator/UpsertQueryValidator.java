package cellarium.db.database.query.validator;

import cellarium.db.database.query.UpsertQuery;

public final class UpsertQueryValidator implements QueryValidator<UpsertQuery> {
    public static final UpsertQueryValidator INSTANCE = new UpsertQueryValidator();

    @Override
    public void validate(UpsertQuery query) {
        TableNameValidator.INSTANCE.validate(query);

        if (query.getValues() == null) {
            throw new NullPointerException("New values is null");
        }

        if (query.getValues().isEmpty()) {
            throw new IllegalArgumentException("Empty values");
        }
    }
}
