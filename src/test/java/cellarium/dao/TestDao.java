package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import cellarium.dao.conf.TestDaoConfig;
import cellarium.dao.entry.AbstractEntry;
import cellarium.dao.entry.Entry;
import cellarium.dao.entry.MemorySegmentEntry;
import cellarium.dao.utils.Utils;
import jdk.incubator.foreign.MemorySegment;

public class TestDao implements Dao<String, Entry<String>> {
    private final MemorySegmentDao memorySegmentDao;

    public TestDao(TestDaoConfig config) throws IOException {
        Path path = Path.of(config.path);
        if (Files.notExists(path)) {
            throw new IllegalStateException("File does not exist: " + path);
        }

        this.memorySegmentDao = new MemorySegmentDao(config);
    }

    @Override
    public Iterator<Entry<String>> get(String fromStr, String toStr) throws IOException {
        final MemorySegment from = Utils.stringToMemorySegment(fromStr);
        final MemorySegment to = Utils.stringToMemorySegment(toStr);

        return new ConverterIterator(memorySegmentDao.get(from, to));
    }

    @Override
    public void upsert(Entry<String> entry) {
        memorySegmentDao.upsert(convert(entry));
    }

    @Override
    public void flush() throws IOException {
        memorySegmentDao.flush();
    }

    @Override
    public void compact() {
        memorySegmentDao.compact();
    }

    @Override
    public void close() throws IOException {
        memorySegmentDao.close();
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        return convert(memorySegmentDao.get(Utils.stringToMemorySegment(key)));
    }

    private static class ConverterIterator implements Iterator<Entry<String>> {
        private final Iterator<MemorySegmentEntry> iterator;

        public ConverterIterator(Iterator<MemorySegmentEntry> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<String> next() {
            return convert(iterator.next());
        }
    }

    private static MemorySegmentEntry convert(Entry<String> entry) {
        if (entry == null) {
            return null;
        }

        return new MemorySegmentEntry(
                Utils.stringToMemorySegment(entry.getKey()),
                entry.getValue() == null ? null : Utils.stringToMemorySegment(entry.getValue()),
                System.currentTimeMillis());
    }

    private static Entry<String> convert(MemorySegmentEntry entry) {
        if (entry == null) {
            return null;
        }

        return new AbstractEntry<>(
                Utils.memorySegmentToString(entry.getKey()),
                Utils.memorySegmentToString(entry.getValue())) {
        };
    }
}
