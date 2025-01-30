package cellarium.db.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public abstract class ASerializer<T> implements Serializer<T, InputStream, OutputStream> {
    private final int version;
    private final Map<Integer, DataSerializer<T>> serializers;

    protected ASerializer(Map<Integer, DataSerializer<T>> serializers) {
        this.serializers = serializers;
        this.version = serializers.keySet().stream().mapToInt(i -> i).max().orElseThrow();
    }

    @Override
    public final T read(InputStream is) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(is);
        final int version = dataInputStream.readInt();

        return getSerializer(version).read(dataInputStream);
    }

    @Override
    public final void write(T obj, OutputStream os) throws IOException {
        final DataOutputStream dataOutputStream = new DataOutputStream(os);
        dataOutputStream.writeInt(version);

        getSerializer(version).write(obj, dataOutputStream);
    }

    private DataSerializer<T> getSerializer(int version) {
        final DataSerializer<T> serializer = serializers.get(version);
        if (serializer == null) {
            throw new IllegalArgumentException("Unknown version: " + version);
        }

        return serializer;
    }
}
