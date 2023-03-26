package cellarium.http.conf;

import java.nio.file.Path;
import java.util.Set;
import one.nio.http.HttpServerConfig;

public final class ServerConfig extends HttpServerConfig {
    public int selfPort;
    public String selfUrl;
    public Set<String> clusterUrls;
    public Path workingDir;
    public int threadCount;
    public int memTableSizeBytes = 4 * 1024 * 1024 * 1024;
    public int daoTimeoutMs = 3000;
    public int requestTimeoutMs = 200;
}
