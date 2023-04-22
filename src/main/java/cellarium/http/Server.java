package cellarium.http;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.cluster.Cluster;
import cellarium.http.cluster.ConsistentHashing;
import cellarium.http.cluster.LoadBalancer;
import cellarium.http.cluster.Node;
import cellarium.http.cluster.request.NodeRequest;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.service.DaoHttpService;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.Hash;

public final class Server extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final Cluster cluster;
    private final LoadBalancer loadBalancer;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.cluster = new Cluster(config.selfUrl, config.clusterUrls, config.virtualNodeAmount, new DaoHttpService(dao));
        this.loadBalancer = new LoadBalancer(config.clusterUrls, config.requestHandlerThreadCount, config.nodeTaskLimit);
    }

    @Override
    public synchronized void stop() {
        log.info("Stopping server");

        try {
            loadBalancer.close();
            cluster.close();
        } catch (IOException e) {
            log.error("Fail on stopping", e);
            throw new IllegalStateException(e);
        } finally {
            super.stop();
        }
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isValidRequest(request)) {
            handleDefault(request, session);
            return;
        }

        final String id = request.getParameter(QueryParam.ID);
        if (id == null) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        final Node node = cluster.getNodeByIndex(
                ConsistentHashing.getNodeIndexForHash(Hash.murmur3(id), cluster.getNodeAmount())
        );

        final Runnable task = () -> {
            try {
                session.sendResponse(
                        node.invoke(
                                new NodeRequest(request, id)
                        )
                );
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };

        loadBalancer.scheduleTask(node.getNodeUrl(), task, e -> sendErrorResponse(session, e));
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
    }

    private static boolean isValidRequest(Request request) {
        return ServerConfiguration.V_0_ENTITY_ENDPOINT.equals(request.getPath())
                && ServerConfiguration.SUPPORTED_METHODS.contains(request.getMethod());
    }

    private static void sendErrorResponse(HttpSession session, Throwable e) {
        try {
            log.error("Response is failed ", e);
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY));
        } catch (IOException ie) {
            log.error("Closing session", ie);
            session.close();
        }
    }
}
