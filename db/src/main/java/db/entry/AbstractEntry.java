package db.entry;

public abstract class AbstractEntry<D> implements Entry<D> {
    protected final D key;
    protected final D value;

    protected AbstractEntry(D key, D value) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        this.key = key;
        this.value = value;
    }

    @Override
    public D getKey() {
        return key;
    }

    @Override
    public D getValue() {
        return value;
    }
}
