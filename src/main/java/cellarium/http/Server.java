package cellarium.http;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.handlers.DaoRequestHandler;
import cellarium.http.handlers.RequestCoordinator;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;

public final class Server extends HttpServer {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final MemorySegmentDao dao;
    private final ExecutorService executorService;

    private final RequestHandler requestCoordinator;

    public Server(ServerConfig config) throws IOException {
        super(createServerConfig(config.selfPort));

        final Path workingDir = config.workingDir;
        if (Files.notExists(workingDir)) {
            Files.createDirectory(workingDir);
        }

        this.dao = new MemorySegmentDao(workingDir, config.memTableSizeBytes);
        this.requestCoordinator = new RequestCoordinator(
                new DaoRequestHandler(this.dao),
                config.selfUrl,
                config.clusterUrls
        );

        this.executorService = Executors.newFixedThreadPool(
                config.threadCount
        );
    }

    @Override
    public synchronized void stop() {
        executorService.shutdown();

        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);

            super.stop();
            dao.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

            if (!isValidRequest(request)) {
                session.sendResponse(createInvalidResponse(request));
                return;
            }

            executorService.execute(() -> {
                try {
                    requestCoordinator.handleRequest(request, session);
                } catch (IOException e) {
                    logger.error("Response is failed", e);
                    sendInternalError(session);
                }
            });
        } catch (IOException e) {
            logger.error("Response is failed", e);
            sendInternalError(session);
        }
    }

    private static Response createInvalidResponse(Request request) {
        if (!ServerConfiguration.V_0_ENTITY_ENDPOINT.equals(request.getPath())) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        if (!ServerConfiguration.SUPPORTED_METHODS.contains(request.getMethod())) {
            return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }

        throw new IllegalStateException("Request is valid");
    }

    private static boolean isValidRequest(Request request) {
        return ServerConfiguration.V_0_ENTITY_ENDPOINT.equals(request.getPath())
                && ServerConfiguration.SUPPORTED_METHODS.contains(request.getMethod());
    }

    private static void sendInternalError(HttpSession session) {
        try {
            session.sendResponse(new Response(Response.INTERNAL_ERROR));
        } catch (IOException ex) {
            logger.error("Response is failed", ex);
            session.socket().close();
        }
    }

    private static HttpServerConfig createServerConfig(int port) {
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;

        final HttpServerConfig httpConfig = new HttpServerConfig();
        httpConfig.acceptors = new AcceptorConfig[]{
                acceptor
        };

        httpConfig.closeSessions = true;

        return httpConfig;
    }
}
