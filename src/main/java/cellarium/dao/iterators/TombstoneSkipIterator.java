package cellarium.dao.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;
import cellarium.dao.entry.Entry;

public final class TombstoneSkipIterator<E extends Entry<?>> implements Iterator<E> {
    private final Iterator<E> delegate;
    private final int timeoutMs;
    private final long createdMs;

    private E current;

    public TombstoneSkipIterator(Iterator<E> delegate) {
        this(delegate, Integer.MAX_VALUE);
    }

    public TombstoneSkipIterator(Iterator<E> delegate, int timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("Timeout cannot be negative: " + timeoutMs);
        }

        this.delegate = delegate;
        this.timeoutMs = timeoutMs;
        this.createdMs = System.currentTimeMillis();
        this.current = getNext(delegate);
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    /**
     */
    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final E next = current;
        current = getNext(delegate);

        return next;
    }

    private E getNext(Iterator<E> iterator) throws TimeoutException {
        while (iterator.hasNext()) {
            if (timeoutMs != Integer.MAX_VALUE && System.currentTimeMillis() - createdMs > timeoutMs) {
                throw new TimeoutException("Unable to complete within specified time limit: " + timeoutMs + "ms");
            }

            final E entry = iterator.next();
            if (entry.getValue() != null) {
                return entry;
            }
        }

        return null;
    }

    public static class TimeoutException extends RuntimeException {
        public TimeoutException(String message) {
            super(message);
        }
    }
}
