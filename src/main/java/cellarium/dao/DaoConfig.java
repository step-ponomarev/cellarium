package cellarium.dao;

import java.nio.file.Path;

public final class DaoConfig {
    public Path path;
    public long memtableLimitBytes = 4 * 1024 * 1024 * 1024;
    public int sstablesLimit = 500;
    public int timeoutMs = 3000;
}
