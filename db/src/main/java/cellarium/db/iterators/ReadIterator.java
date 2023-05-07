package cellarium.db.iterators;

import java.util.Iterator;

public class ReadIterator<E> implements Iterator<E> {
    private final Iterator<E> delegate;

    public ReadIterator(Iterator<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        try {
            return delegate.hasNext();
        } catch (Exception e) {
            throw new ReadException(e);
        }
    }

    @Override
    public E next() {
        try {
            return delegate.next();
        } catch (Exception e) {
            throw new ReadException(e);
        }
    }

    public static final class ReadException extends RuntimeException {
        public ReadException(Throwable cause) {
            super(cause);
        }
    }
}
