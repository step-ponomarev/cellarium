package cellarium.db.tombstone;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class TombstoneSkipIterator<E extends Tombstone> implements Iterator<E> {
    private final Iterator<E> delegate;

    private E current;

    public TombstoneSkipIterator(Iterator<E> delegate) {
        if (delegate == null) {
            throw new NullPointerException("Delegate iterator is null");
        }

        this.delegate = delegate;
        this.current = this.getNext(delegate);
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
            if (!entry.isTombstone()) {
                return entry;
            }
        }

        return null;
    }
}
