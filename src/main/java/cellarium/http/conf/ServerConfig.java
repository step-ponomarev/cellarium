package cellarium.http.conf;

import java.util.Set;
import one.nio.http.HttpServerConfig;

public final class ServerConfig extends HttpServerConfig {
    public int selfPort;
    public String selfUrl;
    public Set<String> clusterUrls;
    public int threadCount;
    public int requestTimeoutMs = 200;
}
