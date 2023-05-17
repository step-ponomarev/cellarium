package cellarium.http.cluster.request;

import one.nio.http.Request;

public interface NodeRequestHandler {
    NodeResponse handleReqeust(Request request, String id, long timestamp) throws RequestInvokeException;
}
