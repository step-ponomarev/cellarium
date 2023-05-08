package cellarium.http.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import cellarium.http.cluster.request.LocalRequestHandler;
import cellarium.http.cluster.request.RemoteRequestHandler;
import cellarium.http.service.DaoHttpService;

public final class Cluster implements Closeable {
    private final Node[] nodes;
    private final LocalRequestHandler localRequestHandler;

    public Cluster(String selfUrl, Set<String> clusterUrls, int virtualNodesCount, DaoHttpService daoHttpService) {
        this.localRequestHandler = new LocalRequestHandler(daoHttpService);
        this.nodes = createVirtualNodes(selfUrl, clusterUrls, virtualNodesCount, this.localRequestHandler);
    }

    @Override
    public void close() throws IOException {
        localRequestHandler.close();
    }

    public Node getNodeByIndex(int i) {
        return nodes[i];
    }

    public int getNodeAmount() {
        return nodes.length;
    }

    private static Node[] createVirtualNodes(String selfUrl, Set<String> clusterUrls, int virtualNodesCount, LocalRequestHandler localRequestHandler) {
        if (clusterUrls == null || clusterUrls.isEmpty()) {
            throw new IllegalArgumentException("No urls provided");
        }

        if (virtualNodesCount == 0) {
            throw new IllegalArgumentException("Virtual nodes count cannot be 0");
        }

        final String[] urls = clusterUrls.toArray(String[]::new);
        final Node[] virtualNodes = new Node[urls.length * virtualNodesCount];

        final Map<String, RemoteRequestHandler> remoteHandlers = clusterUrls.stream()
                .filter(u -> !selfUrl.equals(u))
                .collect(Collectors.toMap(UnaryOperator.identity(), RemoteRequestHandler::new));

        // node order for 3 nodes and 2 virual for each: [1, 2, 3, 1, 2, 3]
        for (int i = 0; i < virtualNodes.length; i++) {
            final String nodeUrl = urls[i % urls.length];

            final boolean localNode = nodeUrl.equals(selfUrl);
            virtualNodes[i] = new Node(nodeUrl, localNode ? localRequestHandler : remoteHandlers.get(nodeUrl));
        }

        return virtualNodes;
    }
}
