package cellarium.db.database;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.StringValue;

public final class DataBaseConcurrencyTest extends ADataBaseTest {
    private static final String TABLE_NAME = "test_table";
    private static final TableScheme SCHEME;

    static {
        final ColumnScheme id = new ColumnScheme("id", DataType.STRING);
        final Map<String, DataType> columns = Map.of("value", DataType.INTEGER);
        SCHEME = new TableScheme(id, columns);
    }

    @Test
    public void basicConcurrencyTest() {
        createTable();

        final int iterationCount = 2000;
        final AtomicInteger handledCount = new AtomicInteger(0);
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < iterationCount; i++) {
                final int index = i;

                final StringValue pk = StringValue.of(String.valueOf(index));
                final IntegerValue value = IntegerValue.of(index);

                final Map<String, AValue<?>> values = Map.of("id", pk, "value", value);
                dataBase.insert(TABLE_NAME, values);
                executorService.execute(() -> {
                    final Iterator<Row<AValue<?>, AValue<?>>> range = dataBase.getRange(TABLE_NAME, null, null);

                    for (int j = 0; j < index + 1; j++) {
                        final Row<AValue<?>, AValue<?>> next = range.next();
                        String key = (String) next.getKey().getValue();
                        Integer val = (Integer) next.getValue().get("value").getValue();

                        Assert.assertEquals(j, Integer.parseInt(key));
                        Assert.assertEquals(j, (int) val);
                    }
                    handledCount.incrementAndGet();
                });
            }
            executorService.shutdown();

            Assert.assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertEquals(iterationCount, handledCount.get());
        } catch (InterruptedException e) {
            resetInterruptFlag();
            throw new RuntimeException(e);
        }
    }

    private void resetInterruptFlag() {
        if (!Thread.currentThread().isInterrupted()) {
            return;
        }

        Thread.currentThread().interrupted();
    }

    private void createTable() {
        dataBase.createTable(TABLE_NAME, SCHEME.getPrimaryKey(), SCHEME.getScheme());
    }
}
