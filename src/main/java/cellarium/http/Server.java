package cellarium.http;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.conf.ServiceConfig;
import cellarium.http.handlers.DaoRequestHandler;
import cellarium.http.handlers.HandlerName;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;

public class Server extends HttpServer {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final MemorySegmentDao dao;
    private final ExecutorService executorService;

    public Server(ServiceConfig config) throws IOException {
        super(createServerConfig(config.selfPort(), config.clusterUrls()));

        final Path workingDir = config.workingDir();
        if (Files.notExists(workingDir)) {
            Files.createDirectory(workingDir);
        }

        this.dao = new MemorySegmentDao(workingDir, ServerConfiguration.DAO_INMEMORY_LIMIT_BYTES);
        addRequestHandlers(
                new DaoRequestHandler(this.dao)
        );

        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() / 2
        );
    }

    @Override
    public synchronized void stop() {
        executorService.shutdown();

        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        super.stop();

        try {
            dao.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        try {
            if (executorService.isShutdown()) {
                session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
                return;
            }

            executorService.execute(() -> {
                try {
                    super.handleRequest(request, session);
                } catch (IOException e) {
                    logger.error("Response is failed", e);
                    sendInternalError(session);
                }
            });
        } catch (IOException | RuntimeException e) {
            logger.error("Response is failed", e);
            sendInternalError(session);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        final Set<Integer> methods = ServerConfiguration.SUPPORTED_METHODS_BY_ENDPOINT.get(request.getPath());
        if (methods == null) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        }

        if (methods != null && !methods.contains(request.getMethod())) {
            session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
        }
    }

    private static void sendInternalError(HttpSession session) {
        try {
            session.sendResponse(new Response(Response.INTERNAL_ERROR));
        } catch (IOException ex) {
            logger.error("Response is failed", ex);
            session.socket().close();
        }
    }

    private static HttpServerConfig createServerConfig(int port, Collection<String> clusterUrls) {
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;

        final HttpServerConfig httpConfig = new HttpServerConfig();
        httpConfig.acceptors = new AcceptorConfig[]{
                acceptor
        };

        httpConfig.virtualHosts = createVirtualHosts(clusterUrls);
        httpConfig.closeSessions = true;

        return httpConfig;
    }

    private static Map<String, String[]> createVirtualHosts(Collection<String> clusterUrls) {
        if (clusterUrls == null || clusterUrls.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String[]> virtualHosts = new HashMap<>(1);
        final String[] hosts = clusterUrls.stream()
                .map(url -> URI.create(url).getHost())
                .distinct()
                .toArray(String[]::new);

        virtualHosts.put(HandlerName.DAO_REQUEST_HANDLER, hosts);

        return virtualHosts;
    }
}
