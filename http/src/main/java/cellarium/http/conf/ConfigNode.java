package cellarium.http.conf;

import java.util.Set;
import one.nio.config.Config;

@Config
public class ConfigNode {
    public String url;
    public Set<String> replicas;
}
