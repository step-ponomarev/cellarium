package cellarium.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import cellarium.disk.DiskUtils;
import cellarium.disk.reader.MemorySegmentEntryReader;
import cellarium.disk.reader.Reader;
import cellarium.disk.writer.MemorySegmentEntryWriter;
import cellarium.entry.EntryComparator;
import cellarium.entry.MemorySegmentEntry;

public final class SSTable implements Closeable {
    public static final long TOMBSTONE_TAG = -1;
    private static final String SSTABLE_FILE_NAME = "sstable.data";
    private static final String INDEX_FILE_NAME = "sstable.index";

    private static final String TIMESTAMP_DELIM = "_T_";
    private static final String SSTABLE_DIR_PREFIX = "SSTABLE";

    private final Path path;
    private final long createdTimeMs;

    private final MemorySegment indexMemorySegment;
    private final MemorySegment tableMemorySegment;

    //TODO: perfomance test
    /**
     * Guarantees state when read: alive / closed
     */
    private final ReadWriteLock readCloseLock = new ReentrantReadWriteLock();

    private SSTable(
            Path path,
            MemorySegment indexMemorySegment,
            MemorySegment tableMemorySegment,
            long createdAt
    ) {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path must exist");
        }

        if (!indexMemorySegment.isReadOnly() || !tableMemorySegment.isReadOnly()) {
            throw new IllegalArgumentException("Mapped segments must be read ondly!");
        }

        this.path = path;
        this.indexMemorySegment = indexMemorySegment;
        this.tableMemorySegment = tableMemorySegment;

        this.createdTimeMs = createdAt;
    }

    //TODO: Здесь мы много аллоцируем и перекладываем данные. 
    // Приходится много хранить в памяти. 
    // Можно ли как-то по-другому?
    public static SSTable createInstance(Path path, Iterator<MemorySegmentEntry> data,
                                         int count,
                                         long sizeBytes
    ) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists: " + path);
        }

        final long timestamp = System.currentTimeMillis();
        final Path sstableDir = Files.createDirectory(
                path.resolve(SSTABLE_DIR_PREFIX + createHash(timestamp))
        );

        final MemorySegment mappedSsTable = MemorySegment.mapFile(
                Files.createFile(sstableDir.resolve(SSTABLE_FILE_NAME)),
                0,
                // key + value sizes * data count + data sizeBytes
                (long) Long.BYTES * 2 * count + sizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        final MemorySegment mappedIndex = MemorySegment.mapFile(
                Files.createFile(sstableDir.resolve(INDEX_FILE_NAME)),
                0,
                //data offsets
                (long) Long.BYTES * count,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        flush(data, mappedSsTable, mappedIndex);

        return new SSTable(
                sstableDir,
                mappedIndex.asReadOnly(),
                mappedSsTable.asReadOnly(),
                timestamp
        );
    }

    public static final class DataWithMeta {
        public final Iterator<MemorySegmentEntry> data;
        public final long sizeBytes;
        public final int count;

        public DataWithMeta(Iterator<MemorySegmentEntry> data, long sizeBytes, int count) {
            this.data = data;
            this.sizeBytes = sizeBytes;
            this.count = count;
        }
    }

    public static List<SSTable> wakeUpSSTables(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists: " + path);
        }

        try (Stream<Path> files = Files.list(path)) {
            final List<String> tableDirNames = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_PREFIX))
                    .sorted()
                    .toList();

            final List<SSTable> tables = new ArrayList<>();
            for (String name : tableDirNames) {
                tables.add(SSTable.upInstance(path.resolve(name)));
            }

            return tables;
        }
    }

    private static SSTable upInstance(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists");
        }

        final Path sstableFile = path.resolve(SSTABLE_FILE_NAME);
        final Path indexFile = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(path) || Files.notExists(indexFile)) {
            throw new IllegalArgumentException("Files must exist.");
        }

        final MemorySegment mappedSsTable = MemorySegment.mapFile(
                sstableFile,
                0,
                Files.size(sstableFile),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        final MemorySegment mappedIndex = MemorySegment.mapFile(
                indexFile,
                0,
                Files.size(indexFile),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        return new SSTable(
                path,
                mappedIndex.asReadOnly(),
                mappedSsTable.asReadOnly(),
                System.currentTimeMillis()
        );
    }

    //TODO: Сделать форсированное закрытие даже если кто-то читает таблицу
    @Override
    public void close() {
        readCloseLock.writeLock().lock();

        //TODO: Корректно ли тут?
        try {
            tableMemorySegment.unload();
            tableMemorySegment.force();
            tableMemorySegment.scope().close();

            indexMemorySegment.unload();
            indexMemorySegment.force();
            indexMemorySegment.scope().close();
        } finally {
            readCloseLock.writeLock().unlock();
        }
    }

    private static void flush(Iterator<MemorySegmentEntry> data, MemorySegment sstable, MemorySegment index) {
        long indexOffset = 0;
        long sstableOffset = 0;

        final MemorySegmentEntryWriter writer = new MemorySegmentEntryWriter(sstable, TOMBSTONE_TAG);
        while (data.hasNext()) {
            MemoryAccess.setLongAtOffset(index, indexOffset, sstableOffset);
            indexOffset += Long.BYTES;
            sstableOffset += writer.write(data.next());
        }
    }

    public void removeSSTableFromDisk() throws IOException {
        if (this.tableMemorySegment.scope().isAlive() || this.indexMemorySegment.scope().isAlive()) {
            throw new IllegalStateException("Scope should be closed!");
        }

        DiskUtils.removeDir(this.path);
    }

    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) {
        final long sstableSizeBytes = tableMemorySegment.byteSize();
        if (sstableSizeBytes == 0) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return new MappedIterator(
                    new MemorySegmentEntryReader(
                            tableMemorySegment,
                            TOMBSTONE_TAG
                    )
            );
        }

        final int maxIndex = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;
        final int fromIndex = from == null ? 0 : Math.abs(
                findIndexOfKey(indexMemorySegment, tableMemorySegment, from)
        );

        // В этом сегменте нет нужного ключа
        if (fromIndex > maxIndex) {
            return Collections.emptyIterator();
        }

        final int toIndex = to == null ? maxIndex + 1 : Math.abs(
                findIndexOfKey(indexMemorySegment, tableMemorySegment, to)
        );
        final long fromPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, fromIndex);
        final long toPosition = toIndex > maxIndex ? sstableSizeBytes : MemoryAccess.getLongAtIndex(indexMemorySegment, toIndex);

        return new MappedIterator(
                new MemorySegmentEntryReader(
                        tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition),
                        TOMBSTONE_TAG
                )
        );
    }

    private static int findIndexOfKey(MemorySegment indexMemorySegment, MemorySegment tableMemorySegment, MemorySegment key) {
        if (indexMemorySegment == null || tableMemorySegment == null || key == null) {
            throw new NullPointerException("Arguments cannot be null!");
        }

        int low = 0;
        int high = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            final long keyPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, mid);

            /**
             * Специфика записи данных.
             * см {@link MemorySegmentEntryWriter} и {@link MemorySegmentEntryReader}
             */
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition);
            final MemorySegment current = tableMemorySegment.asSlice(keyPosition + Long.BYTES, keySize);

            final int compareResult = EntryComparator.compareMemorySegments(current, key);
            if (compareResult < 0) {
                low = mid + 1;
            } else if (compareResult > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -low;
    }

    private static String createHash(long timestamp) {
        final int HASH_SIZE = 40;

        StringBuilder hash = new StringBuilder(createTimeMark(timestamp))
                .append("_H_")
                .append(System.nanoTime());

        while (hash.length() < HASH_SIZE) {
            hash.append(0);
        }

        return hash.substring(0, HASH_SIZE);
    }

    private static String createTimeMark(long timestamp) {
        return TIMESTAMP_DELIM + timestamp;
    }

    public long getCreatedTime() {
        return createdTimeMs;
    }

    private final class MappedIterator implements Iterator<MemorySegmentEntry> {
        private final Reader<MemorySegmentEntry> memorySegmentEntryReader;

        public MappedIterator(Reader<MemorySegmentEntry> memorySegmentEntryReader) {
            this.memorySegmentEntryReader = memorySegmentEntryReader;
        }

        @Override
        public boolean hasNext() {
            readCloseLock.readLock().lock();
            try {
                return memorySegmentEntryReader.hasNext();
            } finally {
                readCloseLock.readLock().unlock();
            }
        }

        @Override
        public MemorySegmentEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            readCloseLock.readLock().lock();
            try {
                return memorySegmentEntryReader.read();
            } finally {
                readCloseLock.readLock().unlock();
            }
        }
    }
}
