package cellarium.dao;

import one.nio.http.HttpServerConfig;

public class DaoConfig extends HttpServerConfig {
    public String path;
    public long memtableLimitBytes = 4 * 1024 * 1024 * 1024;
    public int sstablesLimit = 500;
    public int timeoutMs = 3000;
}
