package cellarium.db.database;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.TableDescription;
import cellarium.db.database.types.DataType;
import cellarium.db.database.validation.NameValidator;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class DataBaseCreateTableTest extends ADataBaseTest {
    private static final String VALID_TABLE_NAME = "testTable";

    @Test(expected = NullPointerException.class)
    public void testNullTableName() {
        dataBase.createTable(null, null, null);
    }

    @Test
    public void testTableSuccessCreated() {
        dataBase.createTable(VALID_TABLE_NAME, new ColumnScheme("id", DataType.LONG), List.of(new ColumnScheme("column1", DataType.BOOLEAN)));

        final TableDescription tableScheme = dataBase.describeTable(VALID_TABLE_NAME);
        Assert.assertNotNull(tableScheme);
        Assert.assertEquals(VALID_TABLE_NAME, tableScheme.tableName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShortTableName() {
        final String tableName = "t";
        dataBase.createTable(tableName, new ColumnScheme("id", DataType.LONG), List.of(new ColumnScheme("column1", DataType.BOOLEAN)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLongTableName() {
        final String tableName = IntStream.range(0, NameValidator.MAX_NAME_LEN + 1)
                .mapToObj(i -> "s")
                .collect(Collectors.joining());

        dataBase.createTable(tableName, new ColumnScheme("id", DataType.LONG), List.of(new ColumnScheme("column1", DataType.BOOLEAN)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableNameStartsWithNumber() {
        dataBase.createTable("1Name", new ColumnScheme("id", DataType.LONG), List.of(new ColumnScheme("column1", DataType.BOOLEAN)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableNameStartsWithUnderscore() {
        dataBase.createTable("_start", new ColumnScheme("id", DataType.LONG), List.of(new ColumnScheme("column1", DataType.BOOLEAN)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableNameEndsWithUnderscore() {
        dataBase.createTable("end_", new ColumnScheme("id", DataType.LONG), List.of(new ColumnScheme("column1", DataType.BOOLEAN)));
    }

    @Test
    public void testCreatedTableScheme() {
        final ColumnScheme pk = new ColumnScheme("id", DataType.LONG);
        final List<ColumnScheme> scheme = List.of(new ColumnScheme("column1", DataType.BOOLEAN));

        dataBase.createTable(VALID_TABLE_NAME, pk, scheme);

        final TableDescription tableDescription = dataBase.describeTable(VALID_TABLE_NAME);
        Assert.assertNotNull(tableDescription);

        final ColumnScheme pkFromDB = tableDescription.tableScheme.getPrimaryKey();
        Assert.assertEquals(pk.getName(), pkFromDB.getName());
        Assert.assertEquals(pk.getType(), pkFromDB.getType());

        for (ColumnScheme s : scheme) {
            Assert.assertEquals(
                    s.getType(),
                    tableDescription.tableScheme.getColumnType(s.getName())
            );
        }
    }
}
