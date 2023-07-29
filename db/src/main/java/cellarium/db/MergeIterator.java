package cellarium.db;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Works with sorted iterators only.
 * Each iterator must not contain duplicated
 */
public final class MergeIterator<E> implements Iterator<E> {
    private final Iterator<E> oldDataIterator;
    private final Iterator<E> newDataIterator;
    private final Comparator<E> comparator;

    private E oldValue;
    private E newValue;

    private MergeIterator(final Iterator<E> left, final Iterator<E> right, Comparator<E> comparator) {
        this.oldDataIterator = left;
        this.newDataIterator = right;
        this.comparator = comparator;

        this.oldValue = getNext(oldDataIterator);
        this.newValue = getNext(newDataIterator);
    }

    /**
     * @param iterators  lists of iterators, values must be unique and sorted
     * @param comparator
     * @return iterator with sorted unique values
     */
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
        return oldValue != null || newValue != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No such element");
        }

        final int compareResult = compare(oldValue, newValue);

        if (compareResult == 0) {
            final E next = newValue;
            oldValue = getNext(oldDataIterator);
            newValue = getNext(newDataIterator);

            return next;
        }

        if (compareResult < 0) {
            final E next = oldValue;
            oldValue = getNext(oldDataIterator);

            return next;
        }

        final E next = newValue;
        newValue = getNext(newDataIterator);

        return next;
    }

    private int compare(E oldData, E newData) {
        if (oldData == null && newData == null) {
            throw new IllegalStateException("Everything is null!");
        }

        if (oldData != null && newData != null) {
            return comparator.compare(oldData, newData);
        }

        if (oldData == null) {
            return 1;
        }

        return -1;
    }

    private E getNext(final Iterator<E> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
