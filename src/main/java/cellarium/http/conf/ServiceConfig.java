package cellarium.http.conf;

import java.nio.file.Path;
import java.util.List;

public final class ServiceConfig {
    public final int selfPort;
    public final String selfUrl;
    public final List<String> clusterUrls;
    public final Path workingDir;
    public final int threadCount;

    public ServiceConfig(
            int selfPort,
            String selfUrl,
            List<String> clusterUrls,
            Path workingDir,
            int threadCount
    ) {
        this.selfPort = selfPort;
        this.selfUrl = selfUrl;
        this.clusterUrls = clusterUrls;
        this.workingDir = workingDir;
        this.threadCount = threadCount;
    }
}
