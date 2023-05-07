package cellarium.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import cellarium.dao.conf.TestDaoConfig;
import db.entry.Entry;
import db.entry.MemorySegmentEntry;
import db.utils.MemorySegmentUtils;
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
        final MemorySegment from = MemorySegmentUtils.stringToMemorySegment(fromStr);
        final MemorySegment to = MemorySegmentUtils.stringToMemorySegment(toStr);

        return new ConverterIterator(memorySegmentDao.get(from, to));
    }

    @Override
    public void upsert(Entry<String> entry) {
        memorySegmentDao.upsert(MemorySegmentUtils.convert(entry));
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
        return MemorySegmentUtils.convert(memorySegmentDao.get(MemorySegmentUtils.stringToMemorySegment(key)));
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
            return MemorySegmentUtils.convert(iterator.next());
        }
    }
}
