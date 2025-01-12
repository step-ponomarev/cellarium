package cellarium.db.database.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.IntegerValue;

public final class ColumnFilterIteratorTest {
    private static final int maxCount = 2000;
    private static final String COLUMN_ID = "id";
    private static final String COLUMN_FILTERED = "toFilter";

    @Test
    public void simpleColumnFilterIterator() {
        final Iterator<Row<IntegerValue, IntegerValue>> generatedValues = generateValues(maxCount);
        final ColumnFilterIterator<Row<IntegerValue, IntegerValue>> filterIterator = new ColumnFilterIterator<>(
                generatedValues,
                Collections.singleton(COLUMN_ID)
        );

        testByPredicate(filterIterator, maxCount, (Row<IntegerValue, IntegerValue> r) -> !r.getColumns().containsKey(COLUMN_FILTERED));
    }

    @Test
    public void nullColumnsFilterIterator() {
        final Iterator<Row<IntegerValue, IntegerValue>> generatedValues = generateValues(maxCount);
        final ColumnFilterIterator<Row<IntegerValue, IntegerValue>> filterIterator = new ColumnFilterIterator<>(
                generatedValues,
                null
        );

        testByPredicate(filterIterator, maxCount, (Row<IntegerValue, IntegerValue> r) -> r.getColumns().containsKey(COLUMN_FILTERED));
    }

    @Test
    public void emptyColumnsFilterIterator() {
        final Iterator<Row<IntegerValue, IntegerValue>> generatedValues = generateValues(maxCount);
        final ColumnFilterIterator<Row<IntegerValue, IntegerValue>> filterIterator = new ColumnFilterIterator<>(
                generatedValues,
                Collections.emptySet()
        );

        testByPredicate(filterIterator, maxCount, (Row<IntegerValue, IntegerValue> r) -> r.getColumns().containsKey(COLUMN_FILTERED));
    }

    public static <T> void testByPredicate(Iterator<T> values, int maxCount, Predicate<T> predicate) {
        int count = 0;
        while (count < maxCount) {
            final T next = values.next();
            Assert.assertTrue(
                    predicate.test(next)
            );

            count++;
        }
        Assert.assertFalse(values.hasNext());
        Assert.assertEquals(maxCount, count);
    }

    private static Iterator<Row<IntegerValue, IntegerValue>> generateValues(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    final IntegerValue value = IntegerValue.of(i);
                    return new Row<>(value, Map.of(COLUMN_ID, value, COLUMN_FILTERED, value));
                }).iterator();
    }
}
