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
import cellarium.disk.DiskUtils;
import cellarium.disk.reader.MemorySegmentEntryReader;
import cellarium.disk.reader.Reader;
import cellarium.disk.writer.MemorySegmentEntryWriter;
import cellarium.entry.MemorySegmentEntry;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

public final class SSTable implements Closeable {
    public static final long TOMBSTONE_TAG = -1;
    private static final String SSTABLE_FILE_NAME = "sstable.data";
    private static final String INDEX_FILE_NAME = "sstable.index";

    private static final String TIMESTAMP_DELIM = "_T_";
    private static final String SSTABLE_DIR_PREFIX = "SSTABLE";

    private final Path path;
    private final long createdTimeMs;

    private final Index index;
    private final MemorySegment tableMemorySegment;

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
        this.index = new Index(tableMemorySegment, indexMemorySegment);
        this.tableMemorySegment = tableMemorySegment;

        this.createdTimeMs = createdAt;
    }

    public static SSTable flushAndCreateSSTable(Path path,
                                                Iterator<MemorySegmentEntry> data,
                                                int count,
                                                long sizeBytes
    ) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists: " + path);
        }

        if (!data.hasNext()) {
            throw new IllegalStateException("Data is empty");
        }

        final long timestamp = System.currentTimeMillis();
        final Path ssTableDir = Files.createDirectory(
                path.resolve(SSTABLE_DIR_PREFIX + createHash(timestamp))
        );

        final MemorySegment mappedSsTable = mapFile(
                Files.createFile(ssTableDir.resolve(SSTABLE_FILE_NAME)),
                // key + value sizes * data count + data sizeBytes
                (long) Long.BYTES * 2 * count + sizeBytes);

        final MemorySegment mappedIndex = mapFile(
                Files.createFile(ssTableDir.resolve(INDEX_FILE_NAME)),
                //data offsets
                (long) Long.BYTES * count
        );

        flush(data, mappedSsTable, mappedIndex);

        return new SSTable(
                ssTableDir,
                mappedIndex.asReadOnly(),
                mappedSsTable.asReadOnly(),
                timestamp
        );
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

    @Override
    public void close() {
        readCloseLock.writeLock().lock();

        try {
            tableMemorySegment.unload();
            tableMemorySegment.force();
            tableMemorySegment.scope().close();

            index.close();
        } finally {
            readCloseLock.writeLock().unlock();
        }
    }

    public long getCreatedTime() {
        return createdTimeMs;
    }

    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return new MappedIterator(
                    new MemorySegmentEntryReader(
                            tableMemorySegment,
                            TOMBSTONE_TAG
                    )
            );
        }

        final int maxIndex = index.getMaxIndex();
        final int fromIndex = index.getFromIndex(from);

        if (fromIndex > maxIndex) {
            return Collections.emptyIterator();
        }

        final int toIndex = index.getToIndex(to);
        final long fromPosition = index.getEntryPositionByIndex(fromIndex);
        final long toPosition = toIndex > maxIndex
                ? tableMemorySegment.byteSize()
                : index.getEntryPositionByIndex(toIndex);

        return new MappedIterator(
                new MemorySegmentEntryReader(
                        tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition),
                        TOMBSTONE_TAG
                )
        );
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

    private static void flush(Iterator<MemorySegmentEntry> data, MemorySegment sstable, MemorySegment index) {
        if (!data.hasNext()) {
            throw new IllegalStateException("Flushing data is empty!");
        }

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
        if (this.tableMemorySegment.scope().isAlive()) {
            throw new IllegalStateException("Scope should be closed!");
        }

        DiskUtils.removeDir(this.path);
    }

    private static MemorySegment mapFile(final Path path, long sizeBytes) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalStateException("File is not exists " + path);
        }

        return MemorySegment.mapFile(
                path,
                0,
                sizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );
    }

    private static String createHash(long timestamp) {
        final int hashSize = 40;

        final StringBuilder hash = new StringBuilder(createTimeMark(timestamp))
                .append("_H_")
                .append(System.nanoTime());

        while (hash.length() < hashSize) {
            hash.append(0);
        }

        return hash.substring(0, hashSize);
    }

    private static String createTimeMark(long timestamp) {
        return TIMESTAMP_DELIM + timestamp;
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
