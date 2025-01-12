package cellarium.db.database;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.StringValue;

public final class DataBaseConcurrencyTest extends ADataBaseTest {
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


                final Map<String, AValue<?>> addedValues = addRow(i, STR."Name\{i}", i % 100, i % 2 == 0, System.currentTimeMillis());
                executorService.execute(() -> {
                    final Iterator<Row<AValue<?>, AValue<?>>> range = select(null, null, null);

                    for (int j = 0; j < index + 1; j++) {
                        final Row<AValue<?>, AValue<?>> row = range.next();
                        for (Map.Entry<String, AValue<?>> e : addedValues.entrySet()) {
                            final AValue<?> selectedValue = row.getValue().get(e.getKey());

                            Assert.assertNotNull(selectedValue);
                        }
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
}
