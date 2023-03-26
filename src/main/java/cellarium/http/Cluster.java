package cellarium.http;

import java.util.Set;
import one.nio.util.Hash;

public class Cluster {
    public final String selfUrl;
    public final String[] clusterUrls;

    public Cluster(String selfUrl, Set<String> clusterUrls) {
        this.selfUrl = selfUrl;
        this.clusterUrls = clusterUrls.toArray(String[]::new);
    }

    public String getClusterUrl(String id) {
        if (clusterUrls.length == 1) {
            return clusterUrls[0];
        }

        return clusterUrls[(Hash.murmur3(id) & Integer.MAX_VALUE) % clusterUrls.length];
    }
}
