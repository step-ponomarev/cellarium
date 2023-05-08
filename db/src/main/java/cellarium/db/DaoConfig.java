package cellarium.db;

public class DaoConfig {
    public String path;
    public long memtableTotalSpaceBytes = 4L * 1024 * 1024 * 1024;
    public int sstablesLimit = 500;
    public Integer timeoutMs;
}
