package cellarium.db.database.iterators;

import java.util.Iterator;

import cellarium.db.database.table.TableRow;

public final class TombstoneFilterIterator<T extends TableRow<?>> implements Iterator<T> {
    private final Iterator<T> iterator;
    private T nextValue;

    public TombstoneFilterIterator(Iterator<T> iterator) {
        this.iterator = iterator;
        this.nextValue = null;
        prepareNextValue();
    }

    @Override
    public boolean hasNext() {
        return nextValue != null;
    }

    @Override
    public T next() {
        final T next = this.nextValue;
        prepareNextValue();


        return next;
    }

    private void prepareNextValue() {
        while (this.iterator.hasNext()) {
            nextValue = this.iterator.next();
            if (nextValue.getValue() != null) {
                return;
            }
        }

        nextValue = null;
    }
}
