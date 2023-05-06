package cellarium.sstable.write;

import java.io.IOException;

public interface Writer<V> {
    /**
     * @param value written value
     * @return size in byts of value
     */
    long write(V value) throws IOException;
}
