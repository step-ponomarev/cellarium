package cellarium.http.cluster.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;

public final class RemoteRequestHandler implements NodeRequestHandler {
    private static final String ERROR_MSG = "Remote request is failed";
    private static final Logger log = LoggerFactory.getLogger(RemoteRequestHandler.class);

    private final HttpClient httpClient;

    public RemoteRequestHandler(String nodeUrl) {
        this.httpClient = new HttpClient(new ConnectionString(nodeUrl));
    }

    @Override
    public Response handleReqeust(NodeRequest request) throws RequestInvokeException {
        try {
            return this.httpClient.invoke(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            log.error(ERROR_MSG, e);
            throw new RequestInvokeException(ERROR_MSG, e);
        } catch (Exception e) {
            log.error(ERROR_MSG, e);
            throw new RequestInvokeException(ERROR_MSG, e);
        }
    }
}
