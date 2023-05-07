package http;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import db.MemorySegmentDao;
import http.cluster.ConsistentHashingCluster;
import http.cluster.Node;
import http.conf.ServerConfig;
import http.conf.ServerConfiguration;
import http.handlers.AsyncRequestHandler;
import http.handlers.LocalRequestHandler;
import http.handlers.RemoteRequestHandler;
import http.service.DaoHttpService;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public final class Server extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final AsyncRequestHandler remoteRequestHandler;
    private final AsyncRequestHandler localRequestHandler;

    private final ConsistentHashingCluster consistentHashingCluster;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.consistentHashingCluster = new ConsistentHashingCluster(config.selfUrl, config.clusterUrls, config.virtualNodeAmount);
        this.localRequestHandler = new LocalRequestHandler(new DaoHttpService(dao), config.localThreadAmount);
        this.remoteRequestHandler = new RemoteRequestHandler(this.consistentHashingCluster, config.remoteThreadAmount, config.requestTimeoutMs);
    }

    @Override
    public synchronized void stop() {
        log.info("Stopping server");

        try {
            remoteRequestHandler.close();
            localRequestHandler.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            super.stop();
        }
    }

    //TODO: Дважды ищем ноду - плохо.
    @Override
    protected RequestHandler findHandlerByHost(Request request) {
        if (!isValidRequest(request)) {
            return null;
        }

        final String id = request.getParameter(QueryParam.ID);
        final Node nodeByKey = consistentHashingCluster.getNodeByKey(id);

        return nodeByKey.isLocalNode() ? localRequestHandler : remoteRequestHandler;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        if (!ServerConfiguration.V_0_ENTITY_ENDPOINT.equals(request.getPath())) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        if (!ServerConfiguration.SUPPORTED_METHODS.contains(request.getMethod())) {
            session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            return;
        }

        if (request.getParameter(QueryParam.ID) == null) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
    }

    private static boolean isValidRequest(Request request) {
        if (request.getParameter(QueryParam.ID) == null) {
            return false;
        }

        return ServerConfiguration.V_0_ENTITY_ENDPOINT.equals(request.getPath())
                && ServerConfiguration.SUPPORTED_METHODS.contains(request.getMethod());
    }
}
