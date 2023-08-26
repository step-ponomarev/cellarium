package cellarium.db.database;

import cellarium.db.database.condition.Condition;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.PrimaryKey;
import cellarium.db.table.TableRow;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public final class DataBaseSelectTableTest extends ADataBaseTest {
    @Test
    public void testSelectById() {
        final String tableName = "test";
        final Map<String, DataType> scheme = Map.of("name", DataType.STRING, "age", DataType.INTEGER);
        dataBase.createTable(tableName, new PrimaryKey("id", DataType.INTEGER), scheme);

        dataBase.insert(
                tableName,
                Map.of("id", IntegerValue.of(221), "age", IntegerValue.of(12))
        );


        Iterator<TableRow<AValue<?>>> id = dataBase.select(tableName, Collections.emptySet(), new Condition(
                Map.of("id", new Condition.ValueCandition(IntegerValue.of(221)))));

        System.out.println(id.next());
    }
}
