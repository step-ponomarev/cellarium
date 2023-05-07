package http.handlers;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import http.QueryParam;
import http.cluster.ConsistentHashingCluster;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

public final class RemoteRequestHandler extends AsyncRequestHandler {
    private final ConsistentHashingCluster consistentHashingCluster;
    private final Map<String, HttpClient> nodeClients;
    private final long requestTimeout;

    public RemoteRequestHandler(ConsistentHashingCluster consistentHashingCluster, int threadCount, long requestTimeout) {
        super(Executors.newFixedThreadPool(threadCount));

        if (threadCount < 1) {
            throw new IllegalArgumentException("Invalid thread count: " + threadCount);
        }

        if (requestTimeout < 0) {
            throw new IllegalArgumentException("Timeout is negative: " + requestTimeout);
        }

        this.consistentHashingCluster = consistentHashingCluster;
        this.requestTimeout = requestTimeout;
        this.nodeClients = consistentHashingCluster.getNodeUrls()
                .stream()
                .collect(
                        Collectors.toMap(UnaryOperator.identity(), u -> new HttpClient(new ConnectionString(u)))
                );
    }

    @Override
    protected void handleRequestAsync(Request request, HttpSession session, ExecutorService executorService) throws RejectedExecutionException {
        final String url = consistentHashingCluster.getNodeByKey(request.getParameter(QueryParam.ID)).getNodeUrl();
        final HttpClient node = nodeClients.get(url);
        if (node == null) {
            throw new IllegalStateException("Cluster node does not exist: " + url);
        }

        final CompletableFuture<Response> responseFuture = CompletableFuture.supplyAsync(() -> {
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

        responseFuture.handleAsync((response, e) -> {
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
            } catch (IOException ex) {
                sendErrorResponse(session, ex);
            }

            return null;
        }, executorService);
    }

    private void handleTimeoutException(HttpSession session) {
        try {
            session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        } catch (IOException ie) {
            sendErrorResponse(session, ie);
        }
    }
}
