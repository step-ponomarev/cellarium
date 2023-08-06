package cellarium.db.database;

import cellarium.db.database.query.UpsertQuery;
import org.junit.Test;

import java.util.Collections;

public final class DataBaseUpsertTest extends ADataBaseTest {
    @Test(expected = IllegalStateException.class)
    public void testUpsertInNonExistentTable() {
        dataBase.upsert(
                new UpsertQuery(
                        "does not exist",
                        -1,
                        Collections.emptyMap()
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpsertEmptyValues() {
        dataBase.upsert(
                new UpsertQuery(
                        "test",
                        -1,
                        Collections.emptyMap()
                )
        );
    }
}
