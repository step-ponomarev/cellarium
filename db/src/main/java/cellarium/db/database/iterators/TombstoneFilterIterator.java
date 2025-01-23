package cellarium.db.database.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

import cellarium.db.database.table.TableRow;

public final class TombstoneFilterIterator<T extends TableRow<?>> implements Iterator<T> {
    private final Iterator<T> iterator;
    private T nextValue;
    private boolean hasNext;

    public TombstoneFilterIterator(Iterator<T> iterator) {
        this.iterator = iterator;
        this.nextValue = null;
    }

    @Override
    public boolean hasNext() {
        if (hasNext) {
            return true;
        }

        while (iterator.hasNext()) {
            nextValue = iterator.next();
            if (nextValue.getColumns() == null) {
                continue;
            }

            hasNext = true;
            break;
        }

        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        hasNext = false;

        return this.nextValue;
    }
}
