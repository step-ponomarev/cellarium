package cellarium.db.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface DataSerializer<T> extends Serializer<T, DataInputStream, DataOutputStream> {}
