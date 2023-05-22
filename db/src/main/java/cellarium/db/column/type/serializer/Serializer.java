package cellarium.db.column.type.serializer;

public interface Serializer<D, S> {
    S serialize(D value);

    D deserialize(S serialized);
}
