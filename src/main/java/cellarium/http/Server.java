package cellarium.http;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.service.DaoHttpService;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.pool.PoolException;

public final class Server extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final DaoHttpService daoHttpService;
    private final one.nio.http.RequestHandler requestHandler;
    private final ClusterClient clusterClient;

    private final ExecutorService localExecutorService;
    private final ExecutorService remoteExecutorService;

    private final String selfUrl;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.daoHttpService = new DaoHttpService(dao);
        //TODO: Нормально настроить координатор(на каждый экзикьютор в конфиг количество тредов)
        final int threadCount = config.threadCount == 1 ? 1 : config.threadCount / 2;
        this.localExecutorService = Executors.newFixedThreadPool(threadCount);
        this.remoteExecutorService = Executors.newFixedThreadPool(threadCount);

        this.clusterClient = new ClusterClient(config.selfUrl, config.clusterUrls);
        this.selfUrl = config.selfUrl;

        this.requestHandler = new RequestHandler(config.requestTimeoutMs);
    }

    @Override
    public synchronized void stop() {
        localExecutorService.shutdown();
        remoteExecutorService.shutdown();

        try {
            localExecutorService.awaitTermination(60, TimeUnit.SECONDS);
            remoteExecutorService.awaitTermination(60, TimeUnit.SECONDS);

            super.stop();
            daoHttpService.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected one.nio.http.RequestHandler findHandlerByHost(Request request) {
        if (!isValidRequest(request)) {
            return null;
        }

        return requestHandler;
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

    @Override
    public HttpSession createSession(Socket socket) {
        return new HttpSession(socket, this) {
            @Override
            public synchronized void sendResponse(Response response) throws IOException {
                final boolean requestIsHandledAlready = this.handling == null;
                if (requestIsHandledAlready) {
                    //TODO: Что я тут делаю?)
                    //И почему без этого мы зависаем по полной
                    this.scheduleClose();
                } else {
                    super.sendResponse(response);
                }
            }
        };
    }

    private static boolean isValidRequest(Request request) {
        return ServerConfiguration.V_0_ENTITY_ENDPOINT.equals(request.getPath())
                && ServerConfiguration.SUPPORTED_METHODS.contains(request.getMethod());
    }

    private final class RequestHandler implements one.nio.http.RequestHandler {
        private final int requestTimeout;

        public RequestHandler(int requestTimeout) {
            if (requestTimeout < 0) {
                throw new IllegalArgumentException("Timeout is negative: " + requestTimeout);
            }

            this.requestTimeout = requestTimeout;
        }

        @Override
        public void handleRequest(Request request, HttpSession session) {
            final String id = request.getParameter(QueryParam.ID);
            final String clusterUrl = clusterClient.getClusterUrl(id);

            try {
                final boolean localRequest = selfUrl.equals(clusterUrl);
                if (localRequest) {
                    handleLocalRequest(request, session);
                } else {
                    handleRemoteRequest(clusterUrl, request, session);
                }
            } catch (RejectedExecutionException e) {
                sendErrorResponse(session, e);
            }
        }

        private void handleRemoteRequest(String url, Request request, HttpSession session) throws RejectedExecutionException {
            final HttpClient node = clusterClient.getNode(url);
            if (node == null) {
                throw new IllegalStateException("Cluster node does not exist: " + url);
            }

            CompletableFuture.runAsync(() -> {
                        try {
                            final Response response = node.invoke(request);
                            session.sendResponse(response);
                        } catch (HttpException | IOException | PoolException e) {
                            sendErrorResponse(session, e);
                        } catch (InterruptedException e) {
                            sendErrorResponse(session, e);
                            Thread.currentThread().interrupt();
                        }
                    }, remoteExecutorService)
                    .orTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                    .exceptionallyAsync((ex) -> {
                        if (ex instanceof TimeoutException) {
                            handleTimeoutException(session);
                        } else if (ex != null) {
                            sendErrorResponse(session, ex);
                        }
                        return null;
                    }, remoteExecutorService);
        }

        private void handleLocalRequest(Request request, HttpSession session) throws RejectedExecutionException {
            localExecutorService.execute(() -> {
                final Response response = daoHttpService.handleRequest(request);
                try {
                    session.sendResponse(response);
                } catch (IOException e) {
                    sendErrorResponse(session, e);
                }
            });
        }

        private void handleTimeoutException(HttpSession session) {
            try {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT));
            } catch (IOException ie) {
                sendErrorResponse(session, ie);
            }
        }
    }

    private static void sendErrorResponse(HttpSession session, Throwable e) {
        try {
            log.error("Response is failed ", e);
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
        } catch (IOException ie) {
            log.error("Closing session", ie);
            session.close();
        }
    }
}
