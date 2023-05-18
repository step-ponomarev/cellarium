package cellarium.http.cluster;

import java.util.Objects;
import cellarium.http.cluster.request.NodeRequestHandler;
import cellarium.http.cluster.request.NodeResponse;
import cellarium.http.cluster.request.RequestInvokeException;
import one.nio.http.Request;

public final class Node {
    private final String nodeUrl;
    private final NodeRequestHandler requestHandler;

    public Node(String nodeUrl, NodeRequestHandler requestHandler) {
        this.nodeUrl = nodeUrl;
        this.requestHandler = requestHandler;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public NodeResponse invoke(Request request, String id, long timestamp) throws RequestInvokeException {
        return requestHandler.handleReqeust(request, id, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node)) return false;
        Node node = (Node) o;
        return nodeUrl.equals(node.nodeUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeUrl);
    }
}
