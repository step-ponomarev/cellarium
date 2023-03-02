package cellarium.http.conf;

import java.nio.file.Path;
import java.util.Set;

public final class ServerConfig {
    public final int selfPort;
    public final String selfUrl;
    public final Set<String> clusterUrls;
    public final Path workingDir;
    public final int threadCount;
    public final int memTableSizeBytes;

    public ServerConfig(
            int selfPort,
            String selfUrl,
            Set<String> clusterUrls,
            Path workingDir,
            int memTableSizeBytes,
            int threadCount
    ) {
        this.selfPort = selfPort;
        this.selfUrl = selfUrl;
        this.clusterUrls = clusterUrls;
        this.workingDir = workingDir;
        this.memTableSizeBytes = memTableSizeBytes;
        this.threadCount = threadCount;
    }
}
