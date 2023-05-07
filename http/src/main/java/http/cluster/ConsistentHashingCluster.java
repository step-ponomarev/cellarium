package http.cluster;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import one.nio.util.Hash;

public final class ConsistentHashingCluster {
    private final Node[] nodes;

    public ConsistentHashingCluster(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        this.nodes = createVirtualNodes(selfUrl, clusterUrls, virtualNodesCount);
    }

    public Node getNodeByKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Empty key");
        }

        return nodes[getNodeIndexForHash(Hash.murmur3(key), nodes.length)];
    }

    public Set<String> getNodeUrls() {
        return Stream.of(nodes).map(Node::getNodeUrl).collect(Collectors.toSet());
    }

    static Node[] createVirtualNodes(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        if (clusterUrls == null || clusterUrls.isEmpty()) {
            throw new IllegalArgumentException("No urls provided");
        }

        if (virtualNodesCount == 0) {
            throw new IllegalArgumentException("Virtual nodes count cannot be 0");
        }

        final String[] urls = clusterUrls.toArray(String[]::new);
        final Node[] nodes = new Node[urls.length * virtualNodesCount];

        for (int i = 0; i < nodes.length; i++) {
            final String url = urls[i % urls.length];
            nodes[i] = new Node(url, url.equals(selfUrl));
        }

        return nodes;
    }

    private static final long MAGIC_NUMBER = 2862933555777941757L;

    // Jump Consistent Hash Algorithm
    static int getNodeIndexForHash(long hash, int nodeAmount) {
        if (nodeAmount == 1) {
            return 0;
        }

        long b = -1;
        long j = 0;

        while (j < nodeAmount) {
            b = j;
            hash = hash * MAGIC_NUMBER + 1;
            j = (long) ((b + 1) * (double) (1L << 31) / ((hash >>> 33) + 1));
        }

        return (int) b;
    }
}
