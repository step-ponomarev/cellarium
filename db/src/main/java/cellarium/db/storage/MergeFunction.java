package cellarium.db.storage;

public interface MergeFunction<E> {
    E merge(E from, E to);
}
