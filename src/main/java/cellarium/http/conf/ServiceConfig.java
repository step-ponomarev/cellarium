package cellarium.http.conf;

import java.nio.file.Path;
import java.util.Collection;

public final class ServiceConfig {
    public final int selfPort;
    public final String selfUrl;
    public final Collection<String> clusterUrls;
    public final Path workingDir;
    public final int threadCount;

    public ServiceConfig(
            int selfPort,
            String selfUrl,
            Collection<String> clusterUrls,
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
