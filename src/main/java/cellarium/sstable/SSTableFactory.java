package cellarium.sstable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import cellarium.DiskUtils;
import cellarium.entry.MemorySegmentEntry;
import cellarium.sstable.write.entry.FileChannelEntryWriter;
import cellarium.sstable.write.entry.MemorySegmentEntryWriter;
import cellarium.sstable.write.index.FileChannelIndexWriter;
import cellarium.sstable.write.index.MemorySegmentIndexWriter;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

public final class SSTableFactory {
    private static final String DATA_FILE_NAME = "sstable.data";
    private static final String INDEX_FILE_NAME = "sstable.index";
    private static final String TIMESTAMP_DELIM = "_";

    private SSTableFactory() {}

    public static SSTableWithMeta flush(Path tableDir, Iterator<MemorySegmentEntry> data) throws IOException {
        if (Files.notExists(tableDir)) {
            throw new IllegalArgumentException("Dir is not exists: " + tableDir);
        }

        if (!data.hasNext()) {
            throw new IllegalStateException("Data is empty");
        }

        final Path ssTableDir = createSSTableDir(tableDir);
        try {
            final Path dataFile = Files.createFile(ssTableDir.resolve(DATA_FILE_NAME));
            final Path indexFile = Files.createFile(ssTableDir.resolve(INDEX_FILE_NAME));

            try (final FileChannel dataFileChannel = FileChannel.open(dataFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                 final FileChannel indexFileChannel = FileChannel.open(indexFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                final SSTableEntryWriter entryWriter = new SSTableEntryWriter(
                        new FileChannelIndexWriter(indexFileChannel, SSTable.BYTE_ORDER),
                        new FileChannelEntryWriter(dataFileChannel, SSTable.TOMBSTONE_TAG, SSTable.BYTE_ORDER)
                );

                while (data.hasNext()) {
                    entryWriter.write(data.next());
                }
            }

            final SSTable ssTable = new SSTable(
                    mapFile(indexFile, Files.size(indexFile)).asReadOnly(),
                    mapFile(dataFile, Files.size(dataFile)).asReadOnly()
            );

            return new SSTableWithMeta(ssTable, ssTableDir);
        } catch (IOException e) {
            DiskUtils.removeDir(ssTableDir);
            throw new IOException(e);
        }
    }

    public static SSTableWithMeta flush(Path tableDir, Iterator<MemorySegmentEntry> data, int count, long sizeBytes) throws IOException {
        if (Files.notExists(tableDir)) {
            throw new IllegalArgumentException("Dir is not exists: " + tableDir);
        }

        if (!data.hasNext()) {
            throw new IllegalStateException("Data is empty");
        }

        final Path ssTableDir = createSSTableDir(tableDir);
        try {
            final MemorySegment mappedSsTable = mapFile(
                    Files.createFile(ssTableDir.resolve(DATA_FILE_NAME)),
                    // key + value sizes * data count + data sizeBytes
                    (long) Long.BYTES * 2 * count + sizeBytes);

            final MemorySegment mappedIndex = mapFile(
                    Files.createFile(ssTableDir.resolve(INDEX_FILE_NAME)),
                    //data offsets
                    (long) Long.BYTES * count);

            final SSTableEntryWriter entryWriter = new SSTableEntryWriter(
                    new MemorySegmentIndexWriter(mappedIndex, SSTable.BYTE_ORDER),
                    new MemorySegmentEntryWriter(mappedSsTable, SSTable.TOMBSTONE_TAG, SSTable.BYTE_ORDER)
            );

            while (data.hasNext()) {
                entryWriter.write(data.next());
            }

            final SSTable ssTable = new SSTable(
                    mappedIndex.asReadOnly(),
                    mappedSsTable.asReadOnly()
            );

            return new SSTableWithMeta(ssTable, ssTableDir);
        } catch (IOException e) {
            DiskUtils.removeDir(ssTableDir);
            throw new IOException(e);
        }
    }

    public static List<SSTableWithMeta> wakeUpSSTables(Path tableDir) throws IOException {
        if (Files.notExists(tableDir)) {
            throw new IllegalArgumentException("Dir is not exists: " + tableDir);
        }

        try (Stream<Path> dirs = Files.list(tableDir)) {
            final List<String> tableDirNames = dirs
                    .map(f -> f.getFileName().toString())
                    .sorted()
                    .toList();

            final List<SSTableWithMeta> tables = new ArrayList<>();
            for (String name : tableDirNames) {
                tables.add(upInstance(tableDir.resolve(name)));
            }

            return tables;
        }
    }

    private static Path createSSTableDir(Path tableDir) throws IOException {
        final String ssTableDirName = tableDir.getFileName() + createHash(System.currentTimeMillis());

        return Files.createDirectory(
                tableDir.resolve(
                        ssTableDirName
                )
        );
    }

    private static String createHash(long timestamp) {
        final int hashSize = 40;

        final StringBuilder hash = new StringBuilder(TIMESTAMP_DELIM + timestamp)
                .append("_H_")
                .append(System.nanoTime());

        while (hash.length() < hashSize) {
            hash.append(0);
        }

        return hash.substring(0, hashSize);
    }

    private static SSTableWithMeta upInstance(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists");
        }

        final Path dataFile = path.resolve(DATA_FILE_NAME);
        final Path indexFile = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(path) || Files.notExists(indexFile)) {
            throw new IllegalArgumentException("Files must exist.");
        }

        final SSTable ssTable = new SSTable(
                mapFile(indexFile, Files.size(indexFile)).asReadOnly(),
                mapFile(dataFile, Files.size(dataFile)).asReadOnly()
        );

        return new SSTableWithMeta(ssTable, path);
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
}
