package cellarium.db.sstable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.comparator.AMemorySegmentComparator;
import cellarium.db.comparator.ComparatorFactory;
import cellarium.db.converter.SSTableRowConverter;
import cellarium.db.converter.sstable.SSTableKey;
import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.MemorySegmentRow;

public final class SSTable {
    private final DataMemorySegmentValue dataSegmentValue;
    private final IndexMemorySegmentValue indexSegmentValue; // key, entity offset

    private static final class SSTableRowData {
        private final MemorySegment data;
        private final long offset;

        public SSTableRowData(MemorySegment data, long offset) {
            this.data = data;
            this.offset = offset;
        }
    }

    public SSTable(DataMemorySegmentValue dataSegment, IndexMemorySegmentValue indexSegment) {
        this.indexSegmentValue = indexSegment;
        this.dataSegmentValue = dataSegment;
    }

    public static SSTable of(final Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalStateException("SSTable does not exist: " + path);
        }

        final Path dataFile = path.resolve("data");
        final Path indexFile = path.resolve("index");

        try (final FileChannel dataFileChannel = FileChannel.open(dataFile, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
             final FileChannel indexFileChannel = FileChannel.open(indexFile, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
            long dataOffset = 0;
            final MemorySegment dataMap = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, dataOffset, Files.size(dataFile), MemorySegmentUtils.ARENA_OF_AUTO);
            final MemorySegment indexMap = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, dataOffset, Files.size(indexFile), MemorySegmentUtils.ARENA_OF_AUTO);

            return new SSTable(
                    new DataMemorySegmentValue(dataMap.asSlice(0)),
                    new IndexMemorySegmentValue(indexMap.asSlice(0))
            );
        }
    }

    //TODO: Рефакторинг
    public static SSTable flush(final Path path, List<ColumnScheme> columnSchemes, Iterator<MemorySegmentRow> data) throws IOException {
        Files.createDirectory(path);

        final List<SSTableRowData> rows = new ArrayList<>();
        final SSTableRowConverter ssTableRowConverter = new SSTableRowConverter(
                columnSchemes.stream().map(ColumnScheme::getName).toList()
        );

        long totalSize = 0;
        while (data.hasNext()) {
            final MemorySegment rowMemorySegment = ssTableRowConverter.convert(data.next());

            rows.add(new SSTableRowData(rowMemorySegment, totalSize));
            totalSize += rowMemorySegment.byteSize();
        }

        try (final FileChannel dataFileChannel = FileChannel.open(path.resolve("data"), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
             final FileChannel indexFileChannel = FileChannel.open(path.resolve("index"), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
            long dataOffset = 0;
            final MemorySegment dataMap = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, dataOffset, totalSize, MemorySegmentUtils.ARENA_OF_AUTO);
            final MemorySegment indexMap = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, dataOffset, totalSize, MemorySegmentUtils.ARENA_OF_AUTO);

            for (int i = 0; i < rows.size(); i++) {
                final SSTableRowData sstableRowData = rows.get(i);

                MemorySegment.copy(sstableRowData.data, 0, dataMap, dataOffset, sstableRowData.data.byteSize());
                indexMap.set(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES, sstableRowData.offset);

                dataOffset += sstableRowData.data.byteSize();
            }
        }

        return of(path);
    }

    public MemorySegment getDataRange(SSTableKey from, SSTableKey to) {
        final MemorySegment dataSegment = dataSegmentValue.getMemorySegment();
        if (from == null && to == null) {
            return dataSegment;
        }

        final AMemorySegmentComparator comparator = from == null ? ComparatorFactory.getComparator(to.types.get(0)) : ComparatorFactory.getComparator(from.types.get(0));
        final MemorySegment indexMemorySegment = indexSegmentValue.getMemorySegment();
        if (from == null) {
            int i = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, to.getMemorySegment(), comparator);
            if (i < 0) {
                i = Math.abs(i) - 1;
            }

            final long offset = indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);
            //todo: не хранить офсеты в массиве!
            return dataSegment.asSlice(0, offset);
        }

        if (to == null) {
            final int i = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, from.getMemorySegment(), comparator);
            final long offset = indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) i * Long.BYTES);

            return dataSegment.asSlice(offset);
        }

        //TODO: если ключи равны - вернуть одно значение
        final int iFrom = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, from.getMemorySegment(), comparator);
        final int iTo = MemorySegmentUtils.findIndexOfKey(dataSegmentValue, indexSegmentValue, to.getMemorySegment(), comparator);

        final long fromOffset = MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iTo);
        if (iTo == indexSegmentValue.maxOffsetIndex) {
            return dataSegment.asSlice(fromOffset);
        }

        final long size = iFrom == iTo
                ? MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iFrom + 1) - MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iFrom)
                : MemorySegmentUtils.getOffsetByIndex(indexSegmentValue, iTo) - fromOffset;

        return dataSegment.asSlice(fromOffset, size);
    }
}
