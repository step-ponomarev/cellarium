package cellarium.db.database;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;

public final class DataBaseDeleteTest extends ADataBaseTest {
    @Test
    public void simpleDeletionTest() {
        createTable();

        final int id = 1;
        addRow(id, "Stepan", 21, true, System.currentTimeMillis());

        Iterator<Row<AValue<?>, AValue<?>>> select = select(id, id, null);
        Assert.assertTrue(select.hasNext());

        select.next();
        Assert.assertFalse(select.hasNext());

        delete(id);
        select = select(id, id, null);
        Assert.assertFalse(select.hasNext());
    }
}
