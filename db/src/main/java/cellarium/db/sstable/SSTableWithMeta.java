package cellarium.db.sstable;

import java.nio.file.Path;

public final class SSTableWithMeta {
    public final SSTable ssTable;
    public final Path sstableDir;

    public SSTableWithMeta(SSTable ssTable, Path sstableDir) {
        this.ssTable = ssTable;
        this.sstableDir = sstableDir;
    }
}
