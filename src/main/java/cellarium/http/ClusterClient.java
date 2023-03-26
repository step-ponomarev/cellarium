package cellarium.http;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;

public final class ClusterClient {
    private final String[] clusterUrls;
    private final Map<String, HttpClient> nodes;

    public ClusterClient(String selfUrl, Set<String> clusterUrls) {
        this.clusterUrls = clusterUrls.toArray(String[]::new);
        this.nodes = clusterUrls.stream().filter(u -> !Objects.equals(selfUrl, u))
                .collect(Collectors.toMap(UnaryOperator.identity(), u -> new HttpClient(new ConnectionString(u))));
    }

    public String getClusterUrl(String id) {
        if (clusterUrls.length == 1) {
            return clusterUrls[0];
        }

        return clusterUrls[(Hash.murmur3(id) & Integer.MAX_VALUE) % clusterUrls.length];
    }

    public HttpClient getNode(String url) {
        return nodes.get(url);
    }
}
