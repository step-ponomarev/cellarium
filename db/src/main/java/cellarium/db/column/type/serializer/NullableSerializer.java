package cellarium.db.column.type.serializer;

public final class NullableSerializer<S, D> implements Serializer<S, D> {
    private Serializer<S, D> serializer;

    public NullableSerializer(Serializer<S, D> serializer) {
        if (serializer == null) {
            throw new NullPointerException("Serializer is null");
        }

        this.serializer = serializer;
    }

    @Override
    public D serialize(S value) {
        if (value == null) {
            return null;
        }

        return serializer.serialize(value);
    }

    @Override
    public S deserialize(D serialized) {
        if (serialized == null) {
            return null;
        }

        return serializer.deserialize(serialized);
    }
}
