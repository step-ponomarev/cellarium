package cellarium.http.cluster.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.http.HttpHeader;
import cellarium.http.QueryParam;
import one.nio.http.HttpClient;
import one.nio.http.Request;
import one.nio.net.ConnectionString;

public final class RemoteNodeRequestHandler implements NodeRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(RemoteNodeRequestHandler.class);

    private static final String ERROR_MSG = "Remote request is failed";

    private final HttpClient httpClient;

    public RemoteNodeRequestHandler(String nodeUrl) {
        this.httpClient = new HttpClient(
                new ConnectionString(nodeUrl)
        );
    }

    @Override
    public NodeResponse handleReqeust(Request request, String id, long timestamp) throws RequestInvokeException {
        try {
            return NodeResponse.wrapRemoteResponse(
                    this.httpClient.invoke(
                            prepareRemoteRequest(request, id, timestamp)
                    )
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            log.error(ERROR_MSG, e);
            throw new RequestInvokeException(ERROR_MSG, e);
        } catch (Exception e) {
            log.error(ERROR_MSG, e);
            throw new RequestInvokeException(ERROR_MSG, e);
        }
    }

    private Request prepareRemoteRequest(Request request, String id, long timestamp) {
        final String[] headers = new String[2];
        headers[0] = HttpHeader.TIMESTAMP + ": " + timestamp;

        final byte[] body = request.getBody();
        headers[1] = "Content-Length: " + (body == null ? 0 : body.length);

        final Request currentNodeRequest = this.httpClient.createRequest(
                request.getMethod(),
                request.getPath() + "?" + QueryParam.ID + "=" + id,
                headers
        );

        currentNodeRequest.setBody(body);

        return currentNodeRequest;
    }
}
