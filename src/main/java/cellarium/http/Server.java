package cellarium.http;

import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.cluster.ConsistentHashingCluster;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.handlers.AsyncRequestHandler;
import cellarium.http.handlers.LocalRequestHandler;
import cellarium.http.handlers.RemoteRequestHandler;
import cellarium.http.service.DaoHttpService;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;

public final class Server extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final AsyncRequestHandler remoteRequestHandler;
    private final AsyncRequestHandler localRequestHandler;

    private final ConsistentHashingCluster consistentHashingCluster;
    private final String selfUrl;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        PropertyConfigurator.configure("log4j.properties");
        
        this.consistentHashingCluster = new ConsistentHashingCluster(config.selfUrl, config.clusterUrls, config.virtualNodeAmount);
        this.localRequestHandler = new LocalRequestHandler(new DaoHttpService(dao), config.localThreadAmount);
        this.remoteRequestHandler = new RemoteRequestHandler(this.consistentHashingCluster, config.remoteThreadAmount, config.requestTimeoutMs);
        this.selfUrl = config.selfUrl;
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

//    @Override
//    protected RequestHandler findHandlerByHost(Request request) {
//        if (!isValidRequest(request)) {
//            return null;
//        }
//
//        final String id = request.getParameter(QueryParam.ID);
//        final String clusterUrl = clusterClient.getClusterUrl(id);
//
//        return selfUrl.equals(clusterUrl) ? localRequestHandler : remoteRequestHandler;
//    }

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
}
