package cellarium.http.cluster.request;

import cellarium.http.cluster.NodeResponse;

public interface NodeRequestHandler {
    NodeResponse handleReqeust(NodeRequest request) throws RequestInvokeException;
}
