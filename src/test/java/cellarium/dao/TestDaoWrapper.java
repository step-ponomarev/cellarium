package cellarium.dao;

import java.io.IOException;
import java.util.Iterator;
import cellarium.EntryConverter;
import cellarium.entry.Entry;
import cellarium.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemorySegment;
import test.entry.Utils;

public class TestDaoWrapper implements Dao<String, Entry<String>> {
    private final MemorySegmentDao memorySegmentDao;

    public TestDaoWrapper(MemorySegmentDao memorySegmentDao) {
        this.memorySegmentDao = memorySegmentDao;
    }

    @Override
    public Iterator<Entry<String>> get(String fromStr, String toStr) throws IOException {
        final MemorySegment from = Utils.stringToMemorySegment(fromStr);
        final MemorySegment to = Utils.stringToMemorySegment(toStr);

        return new ConverterIterator(memorySegmentDao.get(from, to));
    }

    @Override
    public void upsert(Entry<String> entry) {
        memorySegmentDao.upsert(EntryConverter.convert(entry));
    }

    @Override
    public void flush() throws IOException {
        memorySegmentDao.flush();
    }

    @Override
    public void compact() throws IOException {
        memorySegmentDao.compact();
    }

    @Override
    public void close() throws IOException {
        memorySegmentDao.close();
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        return EntryConverter.convert(memorySegmentDao.get(Utils.stringToMemorySegment(key)));
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
            return EntryConverter.convert(iterator.next());
        }
    }
}
