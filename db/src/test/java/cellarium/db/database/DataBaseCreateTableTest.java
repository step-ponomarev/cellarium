package cellarium.db.database;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.types.DataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public final class DataBaseCreateTableTest extends ADataBaseTest {
    @Test(expected = NullPointerException.class)
    public void testNullTableName() {
        dataBase.createTable(null, null, null);
    }

    @Test
    public void testTableName() {
        final String tableName = "tableName";
        dataBase.createTable(tableName, new ColumnScheme("id", DataType.LONG), Map.of("column1", DataType.BOOLEAN));
        Assert.assertTrue(
                dataBase.describeTables().containsKey(tableName));
    }
}
