package cellarium.http.handlers;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

public final class RequestCoordinator implements RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestCoordinator.class);
    private final RequestHandler requestHandler;

    private final URI currentURI;
    private final String[] clusterUrls;

    private final Map<String, HttpClient> nodes;

    public RequestCoordinator(RequestHandler requestHandler, String selfUrl, Set<String> clusterUrls) {
        this.requestHandler = requestHandler;
        this.currentURI = URI.create(selfUrl);
        this.clusterUrls = clusterUrls.toArray(String[]::new);
        this.nodes = clusterUrls.stream()
                .filter(u -> !u.equals(selfUrl))
                .collect(Collectors.toMap(UnaryOperator.identity(), u -> new HttpClient(new ConnectionString(u))));
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        final String id = request.getParameter(QueryParam.ID);
        final String clusterUrl = getClusterUrl(id, clusterUrls);
        if (currentURI.toString().equals(clusterUrl)) {
            requestHandler.handleRequest(request, session);
            return;
        }

        try {
            session.sendResponse(
                    nodes.get(clusterUrl).invoke(request)
            );
        } catch (PoolException | HttpException | InterruptedException e) {
            logger.error("Response is failed", e);
            sendInternalError(session);
        }
    }

    private static String getClusterUrl(String id, String[] clusterUrls) {
        if (clusterUrls.length == 1) {
            return clusterUrls[0];
        }

        return clusterUrls[Math.floorMod(id.hashCode(), clusterUrls.length)];
    }

    private static void sendInternalError(HttpSession session) {
        try {
            session.sendResponse(new Response(Response.INTERNAL_ERROR));
        } catch (IOException ex) {
            logger.error("Response is failed", ex);
            session.socket().close();
        }
    }
}
