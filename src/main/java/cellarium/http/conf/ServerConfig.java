package cellarium.http.conf;

import java.util.Set;
import one.nio.config.Config;
import one.nio.http.HttpServerConfig;

@Config
public final class ServerConfig extends HttpServerConfig {
    public int selfPort;
    //http://domain:port
    public String selfUrl;
    public Set<String> clusterUrls;
    public int localThreadAmount = 1;
    public int remoteThreadAmount = 1;
    public int requestTimeoutMs = 200;
    public int virtualNodeAmount = 10;
}
