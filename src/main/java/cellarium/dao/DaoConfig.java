package cellarium.dao;

import java.nio.file.Path;

public final class DaoConfig {
    public final Path path;
    public final long memtableLimitBytes;
    public final int sstablesLimit;

    public DaoConfig(Path path, long memtableLimitBytes, int sstablesLimit) {
        this.path = path;
        this.memtableLimitBytes = memtableLimitBytes;
        this.sstablesLimit = sstablesLimit;
    }
}
