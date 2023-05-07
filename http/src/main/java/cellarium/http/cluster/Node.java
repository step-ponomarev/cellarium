package cellarium.http.cluster;

import cellarium.http.cluster.request.NodeRequest;
import cellarium.http.cluster.request.NodeRequestHandler;
import cellarium.http.cluster.request.RequestInvokeException;
import one.nio.http.Response;

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

    public Response invoke(NodeRequest request) throws RequestInvokeException {
        return requestHandler.handleReqeust(request);
    }
}
