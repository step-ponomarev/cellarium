package cellarium.db.files;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.sstable.SSTable;

public final class DiskComponent {
    private static final String TABLES_DIR = "tables";
    private final Path tables;

    // - cellariumDb
    //   - tables
    //     - table_name1
    //       - meta
    //         - scheme
    //         - meta(last flush time, last compaction time.. etc, sstable count)
    //       - sstables
    //     - table_name2
    //     - table_name3

    // sstables
    public DiskComponent(Path root) throws IOException {
        this.tables = root.resolve(TABLES_DIR);
        if (!Files.exists(this.tables)) {
            Files.createDirectories(this.tables);
        }
    }

    public void createTable(String tableName) throws IOException {
        final Path tablePath = this.tables.resolve(tableName);
        if (Files.exists(tablePath)) {
            throw new IllegalArgumentException(STR."Table is exists: \{tableName}");
        }

        Files.createDirectory(tablePath);
    }

    public void removeTableFromDisk(String tableName) throws IOException {
        final Path tablePath = this.tables.resolve(tableName);
        if (!Files.exists(tablePath)) {
            throw new IllegalArgumentException(STR."Table is not exists: \{tableName}");
        }

        DiskUtils.removeFile(tablePath);
    }

    public SSTable flush(String tableName, Iterator<MemorySegmentRow> memTable, long sizeBytes) {
        if (!memTable.hasNext()) {
            throw new IllegalStateException("Flushing empty data");
        }

        final MemorySegment ssTable = Arena.ofAuto().allocate(sizeBytes);
        while (memTable.hasNext()) {
//            SSTableValueConverter.INSTANCE.convert(memTable.next());
//            MemorySegmentRow next = ;
        }

        return null;
    }
}
