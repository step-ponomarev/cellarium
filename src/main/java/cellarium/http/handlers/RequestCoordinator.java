package cellarium.http.handlers;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public final class RequestCoordinator implements RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestCoordinator.class);

    private final HttpClient client = HttpClient.newHttpClient();
    private final RequestHandler requestHandler;

    private final String selfUrl;
    private final String[] clusterUrls;

    public RequestCoordinator(RequestHandler requestHandler, String selfUrl, String[] clusterUrls) {
        this.requestHandler = requestHandler;
        this.selfUrl = selfUrl;
        this.clusterUrls = clusterUrls;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        final String id = request.getParameter(QueryParam.ID);
        final String clusterUrl = getClusterUrl(id, clusterUrls);
        if (clusterUrl.equals(selfUrl)) {
            requestHandler.handleRequest(request, session);
            return;
        }

        final HttpRequest httpRequest = HttpRequest.newBuilder(
                        URI.create(clusterUrl + request.getURI())
                ).method(request.getMethodName(), HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                .build();

        try {
            final HttpResponse<byte[]> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            session.sendResponse(
                    new Response(String.valueOf(response.statusCode()), response.body())
            );
        } catch (InterruptedException e) {
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
