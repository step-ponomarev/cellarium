package cellarium.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.handlers.CoordinatorRequestHandler;
import cellarium.http.handlers.DaoRequestHandler;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public final class Server extends HttpServer {
    private final MemorySegmentDao dao;
    private final RequestHandler coordinator;

    private final ExecutorService localExecutorService;
    private final ExecutorService remoteExecutorService;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.dao = dao;
        //TODO: Нормально настроить координатор(на каждый экзикьютор в конфиг количество тредов)
        final int threadCount = config.threadCount == 1 ? 1 : config.threadCount / 2;
        this.localExecutorService = Executors.newFixedThreadPool(threadCount);
        this.remoteExecutorService = Executors.newFixedThreadPool(threadCount);
        this.coordinator = new CoordinatorRequestHandler(
                new Cluster(config.selfUrl, config.clusterUrls),
                new DaoRequestHandler(this.dao),
                this.localExecutorService,
                this.remoteExecutorService
        );
    }

    @Override
    public synchronized void stop() {
        localExecutorService.shutdown();
        remoteExecutorService.shutdown();

        try {
            localExecutorService.awaitTermination(60, TimeUnit.SECONDS);
            remoteExecutorService.awaitTermination(60, TimeUnit.SECONDS);

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
    protected RequestHandler findHandlerByHost(Request request) {
        if (!isValidRequest(request)) {
            return null;
        }

        return coordinator;
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
