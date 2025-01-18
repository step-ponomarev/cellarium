package cellarium.db.files;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.regex.Pattern;

import cellarium.db.database.table.MemorySegmentRow;
import cellarium.db.database.table.TableScheme;
import cellarium.db.sstable.SSTable;

//TODO:
// 1) Сохранение меты по каждой таблице
// 2) Сохранение меты по всем таблицам
// 3) Загрузка всех SSTables с диска
public final class DiskComponent {
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("^sstable_(\\d+)$");
    private static final String TABLES_DIR = "tables";
    private static final String META_FILE = "meta";
    private final Path tables;
    private final Path metaFile;
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
        if (Files.notExists(this.tables)) {
            Files.createDirectories(this.tables);
        }

        this.metaFile = root.resolve(META_FILE);
    }

//    public void writeMeta(Meta meta) {
//        try (ObjectOutputStream ois = new ObjectOutputStream(new FileOutputStream(metaFile.toFile()))) {
//            ois.writeObject(meta);
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public Meta readMeta() {
//        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(metaFile.toFile()))) {
//            return (Meta) ois.readObject();
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void createTable(String tableName, TableScheme tableScheme) throws IOException {
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
