package cellarium.http.cluster.request;

import one.nio.http.Response;

public interface NodeRequestHandler {
    Response handleReqeust(NodeRequest request) throws RequestInvokeException;
}
