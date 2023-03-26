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
        this.delegate = delegate;
        this.timeoutMs = timeoutMs;
        this.createdMs = System.currentTimeMillis();
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    /**
     * @throws TimeoutException
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
        int tombstonesSkippedAmount = 0;
        while (iterator.hasNext()) {
            final E entry = iterator.next();
            if (entry.getValue() != null) {
                return entry;
            }

            tombstonesSkippedAmount++;
            if (System.currentTimeMillis() - createdMs > timeoutMs) {
                throw new TimeoutException("Timeout while skipped tombstones, skipped: " + tombstonesSkippedAmount + "tombstone");
            }
        }

        return null;
    }
    
    private static class TimeoutException extends RuntimeException {
        public TimeoutException(String message) {
            super(message);
        }
    }
}
