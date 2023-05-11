package cellarium.http.conf;

import java.util.Set;
import one.nio.config.Config;
import one.nio.http.HttpServerConfig;

@Config
public final class ServerConfig extends HttpServerConfig {
    public int selfPort;
    //http://domain:port
    public String selfUrl;
    public Set<ConfigNode> cluster;
    public int requestHandlerThreadCount = 1;
    public int virtualNodeAmount = 10;
    public int maxTasksPerNode = 1;
}
