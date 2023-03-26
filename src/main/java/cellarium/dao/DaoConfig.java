package cellarium.dao;

public class DaoConfig {
    public String path;
    public long memtableLimitBytes = 4 * 1024 * 1024 * 1024;
    public int sstablesLimit = 500;
    public int timeoutMs = Integer.MAX_VALUE;
}
