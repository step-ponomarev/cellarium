package cellarium.http.cluster.request;

import java.io.IOException;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

public final class RemoteRequestHandler implements NodeRequestHandler {
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
            throw new RequestInvokeException("Remote request is failed", e);
        } catch (PoolException | IOException | HttpException e) {
            throw new RequestInvokeException("Remote request is failed", e);
        }
    }
}
