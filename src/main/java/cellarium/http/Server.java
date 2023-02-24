package cellarium.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;

public class Server extends HttpServer {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final MemorySegmentDao dao;
    private final ExecutorService executorService;
    private final HttpClient client = HttpClient.newHttpClient();

    private final String selfUrl;
    private final String[] clusterUrls;

    public Server(ServiceConfig config) throws IOException {
        super(createServerConfig(config.selfPort, config.clusterUrls));

        this.selfUrl = config.selfUrl;
        this.clusterUrls = config.clusterUrls.stream().toArray(String[]::new);

        final Path workingDir = config.workingDir;
        if (Files.notExists(workingDir)) {
            Files.createDirectory(workingDir);
        }

        this.dao = new MemorySegmentDao(workingDir, ServerConfiguration.DAO_INMEMORY_LIMIT_BYTES);
        addRequestHandlers(
                new DaoRequestHandler(this.dao)
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

            final Response defaultResponse = createResponseIfBadRequest(request);
            if (defaultResponse != null) {
                session.sendResponse(defaultResponse);
                return;
            }

            final String id = request.getParameter(QueryParam.ID);
            final String clusterUrl = getClusterUrl(id, clusterUrls);
            if (!clusterUrl.equals(selfUrl)) {
                
                final HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(clusterUrl + request.getURI()))
                        .method(request.getMethodName(), HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                        .build();

                //TODO: Делаем запрос на другую ноду за данными? Может лучше просить клиента сходить на другую ноду? Наверное неет
                final CompletableFuture<HttpResponse<byte[]>> httpResponseCompletableFuture = client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
                final HttpResponse<byte[]> httpResponse = httpResponseCompletableFuture.get();
                //TODO: Теряем хедеры?
                session.sendResponse(new Response(String.valueOf(httpResponse.statusCode()), httpResponse.body()));

                return;
            }

            executorService.execute(() -> {

                //TODO: Проверяем валидный ли запрос
                // Проверяем на этой ноде должны мы это делать или на другой
                // Если на этой, то хендлим запрос через локальный хендлер
                // Если на другой, то отпраляем по HTTP Client на другую ноду в кластере
                try {
                    final RequestHandler handlerByHost = findHandlerByHost(request);
                    handlerByHost.handleRequest(request, session);
                } catch (IOException e) {
                    logger.error("Response is failed", e);
                    sendInternalError(session);
                }
            });
        } catch (IOException | RuntimeException | ExecutionException | InterruptedException e) {
            logger.error("Response is failed", e);
            sendInternalError(session);
        }
    }

    private static String getClusterUrl(String id, String[] clusterUrls) {
        if (clusterUrls.length == 1) {
            return clusterUrls[0];
        }

        int i = Math.floorMod(id.hashCode(), clusterUrls.length);
        return clusterUrls[0];
    }

    /**
     * @param request
     * @return null if valid request
     */
    public static Response createResponseIfBadRequest(Request request) {
        final Set<Integer> methods = ServerConfiguration.SUPPORTED_METHODS_BY_ENDPOINT.get(request.getPath());
        if (methods == null) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        if (!methods.contains(request.getMethod())) {
            return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }

        return null;
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

        //TODO: Нужно ли тут указывать все хосты в кластере?
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
