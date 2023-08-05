package cellarium.db.database.query.validator;

import cellarium.db.database.query.Query;

public interface QueryValidator<Q extends Query> {
    void validate(Q query);
}
