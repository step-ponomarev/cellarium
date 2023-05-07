package cellarium.db.iterators;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class MergeIterator<E> implements Iterator<E> {
    private final Iterator<E> oldDataIterator;
    private final Iterator<E> newDataIterator;
    private final Comparator<E> comparator;

    private E oldEntry;
    private E newEntry;

    private MergeIterator(final Iterator<E> left, final Iterator<E> right, Comparator<E> comparator) {
        this.oldDataIterator = left;
        this.newDataIterator = right;
        this.comparator = comparator;

        this.oldEntry = getElement(oldDataIterator);
        this.newEntry = getElement(newDataIterator);
    }

    public static <E> Iterator<E> of(List<Iterator<E>> iterators, Comparator<E> comparator) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        final int size = iterators.size();
        if (size == 1) {
            return iterators.get(0);
        }

        return new MergeIterator<>(
                of(iterators.subList(0, size / 2), comparator),
                of(iterators.subList(size / 2, size), comparator),
                comparator
        );
    }

    @Override
    public boolean hasNext() {
        return oldEntry != null || newEntry != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No such element");
        }

        final int compareResult = comparator.compare(oldEntry, newEntry);

        if (compareResult == 0) {
            final E next = newEntry;
            oldEntry = getElement(oldDataIterator);
            newEntry = getElement(newDataIterator);

            return next;
        }

        if (compareResult < 0) {
            final E next = oldEntry;
            oldEntry = getElement(oldDataIterator);

            return next;
        }

        final E next = newEntry;
        newEntry = getElement(newDataIterator);

        return next;
    }

    private E getElement(final Iterator<E> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
