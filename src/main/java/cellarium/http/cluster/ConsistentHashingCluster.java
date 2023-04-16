package cellarium.http.cluster;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import one.nio.util.Hash;

public final class ConsistentHashingCluster {
    private final VirtualNode[] nodes;

    public ConsistentHashingCluster(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        this.nodes = createSortedVirtualNodes(selfUrl, clusterUrls, virtualNodesCount);
    }

    static VirtualNode[] createSortedVirtualNodes(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        if (clusterUrls == null || clusterUrls.isEmpty()) {
            throw new IllegalArgumentException("No urls provided");
        }

        if (virtualNodesCount == 0) {
            throw new IllegalArgumentException("Virtual nodes count cannot be 0");
        }

        final Set<VirtualNode> nodes = new HashSet<>();
        for (String url : clusterUrls) {
            final boolean nodeIsLocal = url.equals(selfUrl);
            for (int i = 0; i < virtualNodesCount; i++) {
                final String virtualUrl = url + "_" + i;
                nodes.add(
                        new VirtualNode(url, nodeIsLocal, Hash.murmur3(virtualUrl))
                );
            }
        }

        return nodes.stream()
                .sorted(Comparator.comparingInt(VirtualNode::hashCode))
                .toArray(VirtualNode[]::new);
    }

    static int getNodeIndexForHash(VirtualNode[] nodes, int hash) {
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

        VirtualNode currentNode = null;
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
