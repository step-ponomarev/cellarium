package cellarium.db.database;

import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.UpsertQuery;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.TypedValue;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public final class DataBaseUpsertTest extends ADataBaseTest {
        @Test(expected = IllegalArgumentException.class)
        public void testUpsertWithBadTableName() {
                dataBase.upsert(
                                new UpsertQuery(
                                                "bad table name",
                                                -1,
                                                Collections.emptyMap()));
        }

        @Test(expected = IllegalArgumentException.class)
        public void testUpsertEmptyValues() {
                dataBase.upsert(
                                new UpsertQuery(
                                                "test",
                                                -1,
                                                Collections.emptyMap()));
        }

        @Test(expected = IllegalStateException.class)
        public void testUpsertInNonExistentTable() {
                dataBase.upsert(
                                new UpsertQuery(
                                                "noTable",
                                                -1,
                                                Map.of("kek", new TypedValue<>(12))));
        }

        @Test(expected = IllegalStateException.class)
        public void testUpsertBadTypeColumn() {
                dataBase.createTable(
                                new CreateTableQuery("test", Map.of("kek", DataType.BOOLEAN)));

                dataBase.upsert(
                                new UpsertQuery(
                                                "test",
                                                -1,
                                                Map.of("kek", new TypedValue<>(12))));
        }

        @Test
        public void testValidUpsert() {
                dataBase.createTable(
                                new CreateTableQuery("test", Map.of("kek", DataType.BOOLEAN)));

                dataBase.upsert(
                                new UpsertQuery(
                                                "test",
                                                1,
                                                Map.of("kek", new TypedValue<>(false))));
        }
}
