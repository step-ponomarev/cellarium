package cellarium.db.database;

import cellarium.db.database.condition.Condition;
import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.PrimaryKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

public final class DataBaseSelectTableTest extends ADataBaseTest {
    @Test
    public void testSelectById() {
        final String tableName = "test";
        final Map<String, DataType> scheme = Map.of("name", DataType.STRING, "age", DataType.INTEGER);
        dataBase.createTable(tableName, new PrimaryKey("id", DataType.INTEGER), scheme);

        final IntegerValue id = IntegerValue.of(221);
        final IntegerValue age = IntegerValue.of(12);

        dataBase.insert(
                tableName,
                Map.of("id", id, "age", age)
        );

        final Iterator<? extends Row<AValue<?>>> rows = dataBase.select(
                tableName,
                null,
                new Condition(id, null)
        );

        Assert.assertTrue(rows.hasNext());
        final Row<AValue<?>> row = rows.next();
        final Map<String, AValue<?>> columns = row.getValue();
        
    }
}
