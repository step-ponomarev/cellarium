package cellarium.db.database;

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

public final class DataBaseConcurrencyTest extends ADataBaseTest {
    @Test
    public void basicConcurrencyInsertTest() {
        createTable();

        final int iterationCount = 2000;
        final AtomicInteger handledCount = new AtomicInteger(0);
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < iterationCount; i++) {
                final int index = i;
                final Map<String, AValue<?>> addedValues = insertRow(i, STR."Name\{i}", i % 100, i % 2 == 0, System.currentTimeMillis());
                executorService.execute(() -> {
                    final Iterator<Row<AValue<?>, AValue<?>>> range = select(null, null, null);

                    for (int j = 0; j < index + 1; j++) {
                        final Row<AValue<?>, AValue<?>> row = range.next();
                        for (Map.Entry<String, AValue<?>> e : addedValues.entrySet()) {
                            final AValue<?> selectedValue = row.getColumns().get(e.getKey());

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

    @Test
    public void basicConcurrencyReplaceTest() {
        createTable();

        final String replacedName = "replaced name";
        final Long replacedBirthday = -1L;

        final int iterationCount = 2000;
        final AtomicInteger handledCount = new AtomicInteger(0);
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < iterationCount; i++) {
                final int index = i;
                insertRow(i, STR."Name\{i}", i % 100, i % 2 == 0, System.currentTimeMillis());
                executorService.execute(() -> {
                    insertRow(index, replacedName, index % 100, index % 2 == 0, replacedBirthday);
                    handledCount.incrementAndGet();
                });
            }

            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertEquals(iterationCount, handledCount.get());

            int count = 0;
            final Iterator<Row<AValue<?>, AValue<?>>> select = select(null, null, null);
            while (select.hasNext()) {
                final Map<String, AValue<?>> columns = select.next().getColumns();
                Assert.assertEquals(replacedName, columns.get(COLUMN_NAME).getValue());
                final Long value = (Long) columns.get(COLUMN_BIRTHDAY).getValue();
                Assert.assertEquals(replacedBirthday, value);

                count++;
            }
            Assert.assertEquals(iterationCount, count);
        } catch (InterruptedException e) {
            resetInterruptFlag();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void basicConcurrencyDeletionTest() {
        createTable();

        final int iterationCount = 2000;
        final AtomicInteger handledCount = new AtomicInteger(0);
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < iterationCount; i++) {
                final int index = i;
                insertRow(i, STR."Name\{i}", i % 100, i % 2 == 0, System.currentTimeMillis());
                executorService.execute(() -> {
                    delete(index);
                    handledCount.incrementAndGet();
                });
            }

            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertEquals(iterationCount, handledCount.get());
            Assert.assertFalse(select(null, null, null).hasNext());
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
