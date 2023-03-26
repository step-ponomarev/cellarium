package cellarium.http.handlers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import cellarium.http.Cluster;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;

public final class CoordinatorRequestHandler extends ARequestHandler {
    private final Cluster cluster;
    private final RequestHandler localRequestHandler;
    private final ExecutorService localExecutorService;
    private final ExecutorService remoteExecutorService;

    private final Map<String, RequestHandler> requestHandlers;

    public CoordinatorRequestHandler(Cluster cluster, RequestHandler localRequestHandler, ExecutorService localExecutorService, ExecutorService remoteRequestHandler) {
        this.cluster = cluster;
        this.localRequestHandler = localRequestHandler;
        this.localExecutorService = localExecutorService;
        this.remoteExecutorService = remoteRequestHandler;

        this.requestHandlers = createHandlers();
    }

    @Override
    protected void doHandleRequest(Request request, HttpSession session) throws RequestHandlingException {
        final String id = request.getParameter(QueryParam.ID);

        try {
            requestHandlers.get(cluster.getClusterUrl(id)).handleRequest(request, session);
        } catch (IOException e) {
            throw new RequestHandlingException(e);
        }
    }

    private Map<String, RequestHandler> createHandlers() {
        if (cluster.clusterUrls.length == 1) {
            return Collections.singletonMap(cluster.selfUrl, new ExecutorRequestHandler(localExecutorService, localRequestHandler));
        }

        final Map<String, RequestHandler> handlers = new HashMap<>((int) (cluster.clusterUrls.length * 1.25 + 1));
        for (String url : cluster.clusterUrls) {
            // Ситуация: Много локальных запросов и локальны экзикьютор захлебывается
            // если мы в одном экзекьюторе будем обрабатывать и ремоут и локальные запросы,
            // то координация на другие ноды так же будет отваливаться в этом случае
            if (url.equals(cluster.selfUrl)) {
                handlers.put(url, new ExecutorRequestHandler(localExecutorService, localRequestHandler));
                continue;
            }

            handlers.put(url, new ExecutorRequestHandler(remoteExecutorService, new RemoteRequestHandler(url)));
        }

        return handlers;
    }
}
