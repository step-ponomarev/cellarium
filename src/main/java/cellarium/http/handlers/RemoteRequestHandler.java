package cellarium.http.handlers;

import java.io.IOException;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

public final class RemoteRequestHandler extends ARequestHandler {
    private final HttpClient node;

    public RemoteRequestHandler(String nodeUrl) {
        this.node = new HttpClient(new ConnectionString(nodeUrl));
    }

    @Override
    protected void doHandleRequest(Request request, HttpSession session) throws RequestHandlingException {
        try {
            session.sendResponse(
                    node.invoke(request)
            );
        } catch (IOException | PoolException | HttpException e) {
            throw new RequestHandlingException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RequestHandlingException(e);
        }
    }
}
