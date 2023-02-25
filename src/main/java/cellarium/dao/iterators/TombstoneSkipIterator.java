package cellarium.dao.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;
import cellarium.dao.entry.Entry;

public class TombstoneSkipIterator<E extends Entry<?>> implements Iterator<E> {
    private final Iterator<E> delegate;
    private E current;

    public TombstoneSkipIterator(Iterator<E> delegate) {
        this.delegate = delegate;
        this.current = getNext(delegate);
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final E next = current;
        current = getNext(delegate);

        return next;
    }

    private E getNext(Iterator<E> iterator) {
        while (iterator.hasNext()) {
            final E entry = iterator.next();
            if (entry.getValue() != null) {
                return entry;
            }
        }

        return null;
    }
}
