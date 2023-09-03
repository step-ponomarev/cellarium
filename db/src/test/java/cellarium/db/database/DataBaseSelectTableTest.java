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
        final String nameColumnName = "nameColumnName";
        final String ageColumnName = "age";
        final String idColumnName = "id";

        final Map<String, DataType> scheme = Map.of(nameColumnName, DataType.STRING, ageColumnName, DataType.INTEGER);
        dataBase.createTable(tableName, new PrimaryKey(idColumnName, DataType.INTEGER), scheme);

        final int idValue = 221;
        final int ageValue = 12;
        final IntegerValue id = IntegerValue.of(idValue);
        final IntegerValue age = IntegerValue.of(ageValue);

        final Map<String, AValue<?>> addedValues = Map.of(idColumnName, id, ageColumnName, age);
        dataBase.insert(
                tableName,
                addedValues
        );

        final Iterator<? extends Row<AValue<?>, AValue<?>>> rows = dataBase.select(
                tableName,
                null,
                new Condition(id, null)
        );

        Assert.assertTrue(rows.hasNext());

        final Row<AValue<?>, AValue<?>> row = rows.next();
        final Map<String, AValue<?>> columns = row.getValue();
        for (Map.Entry<String, AValue<?>> v : addedValues.entrySet()) {
            final AValue<?> value = columns.get(v.getKey());
            Assert.assertNotNull(value);
            Assert.assertEquals(v.getValue().getValue(), value.getValue());
        }
    }
}
