package cellarium.http.conf;

import java.nio.file.Path;
import java.util.Collection;

public final class ServerConfig {
    public final int selfPort;
    public final String selfUrl;
    public final Collection<String> clusterUrls;
    public final Path workingDir;
    public final int threadCount;
    public final int memTableSizeBytes;

    public ServerConfig(
            int selfPort,
            String selfUrl,
            Collection<String> clusterUrls,
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
