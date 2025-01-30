package cellarium.db.database;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;

public final class DataBaseFlushTest extends ADataBaseTest {
    @Test
    public void testDataExistedAfterClose() {
        final RuntimeException[] exceptions = new RuntimeException[1];

        testWithFlush(() -> {
            try {
                dataBase.close();
                init();
            } catch (IOException e) {
                exceptions[0] = new RuntimeException(e);
            }
        });

        if (exceptions[0] != null) {
            throw exceptions[0];
        }
    }

    @Test
    public void testDataExistedAfterFlush() {
        final RuntimeException[] exceptions = new RuntimeException[1];

        testWithFlush(() -> {
            try {
                dataBase.flush();
                init();
            } catch (IOException e) {
                exceptions[0] = new RuntimeException(e);
            }
        });

        if (exceptions[0] != null) {
            throw exceptions[0];
        }
    }

    @Override
    protected long getMaxBytes() {
        return 50;
    }

    private void testWithFlush(Runnable flush) {
        createTable();

        final int id = 1;
        final Map<String, AValue<?>> addedValues = insertRow(id, "Stepan", 21, true, System.currentTimeMillis());

        try {
            dataBase.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        init();

        final Iterator<Row<AValue<?>, AValue<?>>> selectToDisk = select(id, id, Collections.EMPTY_SET);
        Assert.assertTrue(selectToDisk.hasNext());

        final Row<AValue<?>, AValue<?>> fromDisk = selectToDisk.next();
        for (final Map.Entry<String, AValue<?>> column : addedValues.entrySet()) {
            final String key = column.getKey();
            final AValue<?> value = column.getValue();

            Assert.assertEquals(value, fromDisk.getColumns().get(key));
        }
    }
}
