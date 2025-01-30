package cellarium.db.files;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.Table;
import cellarium.db.database.table.TableScheme;
import cellarium.db.serialization.tabllescheme.TableSchemeSerializer;
import cellarium.db.sstable.SSTable;

//TODO:
// 1) Сохранение меты по каждой таблице
// 2) Сохранение меты по всем таблицам
// 3) Загрузка всех SSTables с диска
public final class DiskComponent {
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("^sstable_(\\d+)$");
    private static final String TABLES_DIR = "tables";
    private static final String TABLE_META_FILE = "table.scheme";
    private static final String META_FILE = "meta";
    private final Path tables;
    int index = 0;

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
        if (Files.notExists(root)) {
            Files.createDirectories(root);
        }

        this.tables = root.resolve(TABLES_DIR);
    }

    public List<Table> getTables() throws IOException {
        return Collections.emptyList();
    }

    public void createTable(String tableName, TableScheme tableScheme) throws IOException {
        final Path tablePath = this.tables.resolve(tableName);
        if (Files.exists(tablePath)) {
            throw new IllegalArgumentException(STR."Table is exists: \{tableName}");
        }

        Files.createDirectories(tablePath);

        final Path metaFile = tablePath.resolve(TABLE_META_FILE);
        TableSchemeSerializer.INSTANCE.write(tableScheme, new FileOutputStream(metaFile.toFile()));
    }

    public void dropTable(String tableName) throws IOException {
        final Path tablePath = this.tables.resolve(tableName);
        if (!Files.exists(tablePath)) {
            throw new IllegalArgumentException(STR."Table is not exists: \{tableName}");
        }

        DiskUtils.removeFile(tablePath);
    }

    public SSTable flush(String tableName, TableScheme tableScheme, Iterator<MemorySegmentRow> memTable) throws IOException {
        if (!memTable.hasNext()) {
            throw new IllegalStateException("Flushing empty data");
        }

        final Path tablePath = tables.resolve(tableName);
        if (!Files.exists(tablePath)) {
            Files.createDirectories(tablePath);
        }

        final Path sstableDir = tablePath.resolve(STR."sstable_\{System.currentTimeMillis()}");

        return SSTable.flush(sstableDir, tableScheme.getScheme(), memTable);
    }
}
