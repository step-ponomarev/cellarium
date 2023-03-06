package cellarium.dao.disk.writer;

public interface Writer<V> {
    /**
     * @param value written value
     * @return size in byts of value
     */
    long write(V value);
}
