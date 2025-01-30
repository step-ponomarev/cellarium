package cellarium.db.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer<T, I extends InputStream, O extends OutputStream> {
    T read(I is) throws IOException;

    void write(T obj, O os) throws IOException;
}
