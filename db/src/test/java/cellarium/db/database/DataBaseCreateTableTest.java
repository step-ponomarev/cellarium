package cellarium.db.database;

import cellarium.db.database.query.CreateTableQuery;
import cellarium.db.database.query.GetByIdQuery;
import cellarium.db.database.types.DataType;
import cellarium.db.entry.Entry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class DataBaseCreateTableTest {
    private DataBase dataBase;

    @Before
    public void init() {
        this.dataBase = new CellariumDB();
    }

    @Test(expected = NullPointerException.class)
    public void testNullTableName() {
        dataBase.createTable(
                new CreateTableQuery(
                        null,
                        Map.of("test_column", DataType.LONG)
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTableName() {
        dataBase.createTable(
                new CreateTableQuery(
                        "",
                        null
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlankTableName() {
        dataBase.createTable(
                new CreateTableQuery(
                        "  ",
                        null
                )
        );
    }

    @Test(expected = NullPointerException.class)
    public void testNullTableScheme() {
        dataBase.createTable(
                new CreateTableQuery(
                        "test_table",
                        null
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTableScheme() {
        dataBase.createTable(
                new CreateTableQuery(
                        "test_table",
                        Collections.emptyMap()
                )
        );
    }

    @Test
    public void testTableCreation() {
        final String tableName = "test_table";
        dataBase.createTable(
                new CreateTableQuery(
                        tableName,
                        Map.of("test_column", DataType.LONG)
                )
        );

        Entry<?, ?> entry = dataBase.getById(
                new GetByIdQuery(
                        tableName,
                        Long.MAX_VALUE
                )
        );

        Assert.assertNull(entry);
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateTableCreation() {
        final String tableName = "test_table";

        final CreateTableQuery query = new CreateTableQuery(
                tableName,
                Map.of("test_column", DataType.LONG)
        );

        dataBase.createTable(query);
        dataBase.createTable(query);
    }
}
