package cellarium.http.cluster;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import one.nio.util.Hash;

public final class ConsistentHashingCluster {
    private final Node[] nodes;

    public ConsistentHashingCluster(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        this.nodes = createSortedVirtualNodes(selfUrl, clusterUrls, virtualNodesCount);
    }

    public Node getNodeByKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Empty key");
        }

        return nodes[getNodeIndexForHash(nodes, Hash.murmur3(key))];
    }

    public Set<String> getNodeUrls() {
        return Stream.of(nodes).map(Node::getNodeUrl).collect(Collectors.toSet());
    }

    static Node[] createSortedVirtualNodes(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        if (clusterUrls == null || clusterUrls.isEmpty()) {
            throw new IllegalArgumentException("No urls provided");
        }

        if (virtualNodesCount == 0) {
            throw new IllegalArgumentException("Virtual nodes count cannot be 0");
        }

        return clusterUrls.stream()
                .flatMap(url -> IntStream.range(0, virtualNodesCount).mapToObj(i -> new VirtualNode(url, url.equals(selfUrl), Hash.murmur3(url + "_" + i))))
                .toArray(VirtualNode[]::new);
    }

    static int getNodeIndexForHash(Node[] nodes, int hash) {
        if (nodes == null || nodes.length == 0) {
            throw new IllegalArgumentException("No nodes provided");
        }

        if (nodes.length == 1) {
            return 0;
        }

        if (nodes.length == 2) {
            return nodes[0].hashCode() >= hash ? 0 : 1;
        }

        if (nodes[nodes.length - 1].hashCode() < hash) {
            return 0;
        }

        int left = 0;
        int right = nodes.length - 1;
        int i;

        Node currentNode = null;
        while (left <= right) {
            i = (right + left) >>> 1;

            currentNode = nodes[i];
            final int currentNodeHash = currentNode.hashCode();

            if (currentNodeHash == hash) {
                return i;
            }

            if (currentNodeHash < hash) {
                left = i + 1;
                continue;
            }

            if (i == 0) {
                return i;
            }

            right = i - 1;
            if (nodes[i - 1].hashCode() < hash) {
                return i;
            }
        }

        throw new IllegalStateException("Node is not found");
    }
}
