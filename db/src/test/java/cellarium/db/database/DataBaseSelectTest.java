package cellarium.db.database;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.StringValue;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DataBaseSelectTest extends ADataBaseTest {
    @Test
    public void testSelectById() {
        createTable();

        final int id = 1;
        final Map<String, AValue<?>> addedValues = insertRow(id, "Stepan", 21, true, System.currentTimeMillis());

        Iterator<Row<AValue<?>, AValue<?>>> row = select(id, id, null);
        final Map<String, AValue<?>> columns = row.next().getColumns();
        for (Map.Entry<String, AValue<?>> v : addedValues.entrySet()) {
            final AValue<?> value = columns.get(v.getKey());
            Assert.assertNotNull(value);
            Assert.assertEquals(v.getValue().getValue(), value.getValue());
        }
    }

    @Test
    public void testSelectByIdWithColumnFilter() {
        createTable();

        final List<Map<String, AValue<?>>> addedRows = new ArrayList<>();
        addedRows.add(insertRow(1, "Stepan", 21, true, System.currentTimeMillis()));
        addedRows.add(insertRow(2, "Ilya", 21, true, System.currentTimeMillis()));
        addedRows.add(insertRow(3, "Egor", 21, true, System.currentTimeMillis()));

        final Set<String> filter = Set.of(COLUMN_AGE, COLUMN_NAME);
        for (Map<String, AValue<?>> addedValues : addedRows) {
            final int id = Integer.parseInt(((StringValue) addedValues.get(COLUMN_ID)).getValue());
            final Row<AValue<?>, AValue<?>> row = select(id, id, filter).next();
            final Map<String, AValue<?>> columns = row.getColumns();

            Assert.assertEquals(filter.size(), columns.size());
            for (Map.Entry<String, AValue<?>> v : columns.entrySet()) {
                final AValue<?> value = addedValues.get(v.getKey());
                Assert.assertNotNull(value);

            }
        }
    }


}
