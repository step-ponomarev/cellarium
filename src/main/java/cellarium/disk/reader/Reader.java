package cellarium.disk.reader;

public interface Reader<V> {
    V read();
    boolean hasNext();
}
