package cellarium.http;

import java.io.IOException;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.handlers.AsyncRequestHandler;
import cellarium.http.handlers.LocalRequestHandler;
import cellarium.http.handlers.RemoteRequestHandler;
import cellarium.http.service.DaoHttpService;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public final class Server extends HttpServer {
    private final AsyncRequestHandler remoteRequestHandler;
    private final AsyncRequestHandler localRequestHandler;

    private final ClusterClient clusterClient;
    private final String selfUrl;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.clusterClient = new ClusterClient(config.selfUrl, config.clusterUrls);

        this.localRequestHandler = new LocalRequestHandler(new DaoHttpService(dao), config.localThreadCount);
        this.remoteRequestHandler = new RemoteRequestHandler(this.clusterClient, config.remoteThreadCount, config.requestTimeoutMs);
        this.selfUrl = config.selfUrl;
    }

    @Override
    public synchronized void stop() {
        super.stop();
        
        try {
            localRequestHandler.close();
            remoteRequestHandler.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected RequestHandler findHandlerByHost(Request request) {
        if (!isValidRequest(request)) {
            return null;
        }

        final String id = request.getParameter(QueryParam.ID);
        final String clusterUrl = clusterClient.getClusterUrl(id);

        return selfUrl.equals(clusterUrl) ? localRequestHandler : remoteRequestHandler;
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
}
