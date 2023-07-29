package cellarium.db.storage;

import cellarium.db.entry.Entry;

import java.util.Iterator;

public interface ColumnStorage<PK, E extends Entry<PK, ?>> extends Storage<PK, E> {
    @Override
    default Iterator<E> get(PK from, PK to) {
        return get(from, to, null);
    }

    Iterator<E> get(PK from, PK to, String[] columns);

    @Override
    default E get(PK pk) {
        return get(pk, (String[]) null);
    }

    E get(PK pk, String[] columns);
}
