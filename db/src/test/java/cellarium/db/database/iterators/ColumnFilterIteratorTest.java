package cellarium.db.database.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.database.table.Row;
import cellarium.db.database.types.IntegerValue;

public final class ColumnFilterIteratorTest {
    private static final String COLUMN_ID = "id";
    private static final String COLUMN_FILTERED = "toFilter";

    @Test
    public void simpleColumnFilterIterator() {
        int maxValue = 2000;
        final Iterator<Row<IntegerValue, IntegerValue>> generatedValues = generateValues(maxValue);
        final ColumnFilterIterator<Row<IntegerValue, IntegerValue>> filterIterator = new ColumnFilterIterator<>(
                generatedValues,
                Collections.singleton(COLUMN_ID)
        );

        int count = 0;
        while (count < maxValue) {
            final Row<IntegerValue, IntegerValue> next = filterIterator.next();
            Assert.assertFalse(
                    next.getValue().containsKey(COLUMN_FILTERED)
            );
            count++;
        }
        Assert.assertFalse(filterIterator.hasNext());
        Assert.assertEquals(maxValue, count);
    }

    @Test
    public void nullColumnsFilterIterator() {
        int maxValue = 2000;
        final Iterator<Row<IntegerValue, IntegerValue>> generatedValues = generateValues(maxValue);
        final ColumnFilterIterator<Row<IntegerValue, IntegerValue>> filterIterator = new ColumnFilterIterator<>(generatedValues, null);

        int count = 0;
        while (count < maxValue) {
            final Row<IntegerValue, IntegerValue> next = filterIterator.next();
            Assert.assertTrue(
                    next.getValue().containsKey(COLUMN_FILTERED)
            );
            count++;
        }
        Assert.assertFalse(filterIterator.hasNext());
        Assert.assertEquals(maxValue, count);
    }

    @Test
    public void emptyColumnsFilterIterator() {
        int maxValue = 2000;
        final Iterator<Row<IntegerValue, IntegerValue>> generatedValues = generateValues(maxValue);
        final ColumnFilterIterator<Row<IntegerValue, IntegerValue>> filterIterator = new ColumnFilterIterator<>(generatedValues, Collections.emptySet());

        int count = 0;
        while (count < maxValue) {
            final Row<IntegerValue, IntegerValue> next = filterIterator.next();
            Assert.assertTrue(
                    next.getValue().containsKey(COLUMN_FILTERED)
            );
            count++;
        }
        Assert.assertFalse(filterIterator.hasNext());
        Assert.assertEquals(maxValue, count);
    }

    private static Iterator<Row<IntegerValue, IntegerValue>> generateValues(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    final IntegerValue value = IntegerValue.of(i);
                    return new Row<>(value, Map.of(COLUMN_ID, value, COLUMN_FILTERED, value));
                }).iterator();
    }
}
