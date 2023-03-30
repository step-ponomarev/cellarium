package cellarium.http.handlers;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import cellarium.http.ClusterClient;
import cellarium.http.QueryParam;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;

public final class RemoteRequestHandler extends AsyncRequestHandler {
    private final ClusterClient clusterClient;
    private final long requestTimeout;

    public RemoteRequestHandler(ClusterClient clusterClient, int threadCount, long requestTimeout) {
        super(Executors.newFixedThreadPool(threadCount));

        if (threadCount < 1) {
            throw new IllegalArgumentException("Invalid thread count: " + threadCount);
        }

        if (requestTimeout < 0) {
            throw new IllegalArgumentException("Timeout is negative: " + requestTimeout);
        }

        this.clusterClient = clusterClient;
        this.requestTimeout = requestTimeout;
    }

    @Override
    protected void handleRequestAsync(Request request, HttpSession session, ExecutorService executorService) {
        final String url = clusterClient.getClusterUrl(request.getParameter(QueryParam.ID));
        final HttpClient node = clusterClient.getNode(url);
        if (node == null) {
            throw new IllegalStateException("Cluster node does not exist: " + url);
        }

        try {
            final CompletableFuture<Response> responseCompletableFuture = CompletableFuture.supplyAsync(() -> {
                        try {
                            return node.invoke(request);
                        } catch (HttpException | IOException | PoolException e) {
                            throw new CompletionException(e);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException(e);
                        }
                    }, executorService)
                    .orTimeout(requestTimeout, TimeUnit.MILLISECONDS);

            responseCompletableFuture.handleAsync((response, e) -> {
                try {
                    if (e == null) {
                        session.sendResponse(response);
                        return null;
                    }

                    if (e instanceof TimeoutException) {
                        handleTimeoutException(session);
                        return null;
                    }

                    sendErrorResponse(session, e.getCause());
                    responseCompletableFuture.cancel(true);
                } catch (IOException ex) {
                    sendErrorResponse(session, ex);
                }

                return null;
            }, executorService);
        } catch (RejectedExecutionException e) {
            sendErrorResponse(session, e);
        }
    }

    private void handleTimeoutException(HttpSession session) {
        try {
            session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        } catch (IOException ie) {
            sendErrorResponse(session, ie);
        }
    }
}
