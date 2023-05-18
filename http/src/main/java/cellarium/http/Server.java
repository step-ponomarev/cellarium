package cellarium.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.db.MemorySegmentDao;
import cellarium.http.cluster.Cluster;
import cellarium.http.cluster.ConsistentHashing;
import cellarium.http.cluster.LoadBalancer;
import cellarium.http.cluster.Node;
import cellarium.http.cluster.request.LocalNodeRequestHandler;
import cellarium.http.cluster.request.NodeResponse;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.utils.ReqeustUtils;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.Hash;

public final class Server extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final Cluster cluster;
    private final LoadBalancer loadBalancer;
    private final LocalNodeRequestHandler localNodeRequestHandler;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        final Map<String, Set<String>> urlToReplicas = config.cluster.stream()
                .collect(Collectors.toMap(u -> u.url, u -> u.replicas));

        this.localNodeRequestHandler = new LocalNodeRequestHandler(dao);
        this.cluster = Cluster.createCluster(config.selfUrl, urlToReplicas, config.virtualNodeAmount, localNodeRequestHandler);
        this.loadBalancer = new LoadBalancer(urlToReplicas.keySet(), config.requestHandlerThreadCount, config.maxTasksPerNode);
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isValidRequest(request)) {
            handleDefault(request, session);
            return;
        }

        final Map<String, String> reqeustParams = ReqeustUtils.extractQueryParams(request);
        final String id = reqeustParams.get(QueryParam.ID);
        if (id == null) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        final String quorumStr = reqeustParams.get(QueryParam.QUORUM);
        try {
            //TODO: Bad guarantee
            final boolean requestFromReplica = quorumStr == null;
            final Node candidateNode = getCandidateNode(id, requestFromReplica);

            final Set<String> replicas = cluster.getReplicaUrlsByUrl(
                    candidateNode.getNodeUrl()
            );

            final Supplier<Response> responseSupplier = requestFromReplica
                    ? createLocalResponseSupplier(request, id)
                    : () -> handleDistributedReqeust(request, id, replicas, Integer.parseInt(quorumStr));

            final boolean scheduled = scheduleRequest(candidateNode.getNodeUrl(), session, responseSupplier);
            if (!scheduled) {
                session.sendResponse(
                        new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY)
                );
            }
        } catch (Exception e) {
            sendErrorResponse(session, e);
        }
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
    public synchronized void stop() {
        log.info("Stopping server");

        try {
            loadBalancer.close();
            localNodeRequestHandler.close();
        } catch (IOException e) {
            log.error("Fail on stopping", e);
            throw new IllegalStateException(e);
        } finally {
            super.stop();
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

    private Node getCandidateNode(String id, boolean requestFromReplica) {
        return requestFromReplica
                ? cluster.getNodeByUrl(cluster.getSelfUrl())
                : cluster.getNodeByIndex(ConsistentHashing.getNodeIndexForHash(Hash.murmur3(id), cluster.getVirtualNodeAmount())
        );
    }

    private Supplier<Response> createLocalResponseSupplier(Request request, String id) {
        return () -> {
            final long coordinatorRequestTimestamp = Long.parseLong(
                    ReqeustUtils.getHeader(request, HttpHeader.TIMESTAMP)
            );

            return localNodeRequestHandler.handleReqeust(request, id, coordinatorRequestTimestamp);
        };
    }

    private boolean scheduleRequest(String candidateUrl, HttpSession session, Supplier<Response> responseSupplier) throws RejectedExecutionException {
        final Runnable task = () -> {
            try {
                session.sendResponse(
                        responseSupplier.get()
                );
            } catch (Exception e) {
                sendErrorResponse(session, e);
            }
        };

        return loadBalancer.scheduleTask(
                candidateUrl,
                task
        );
    }

    private Response handleDistributedReqeust(Request request, String id, Set<String> replicaUrls, int quorum) {
        final String[] replicas = loadBalancer.sortByLoadFactor(replicaUrls);
        if (replicas.length < quorum) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }

        final long timestamp = System.currentTimeMillis();
        final NodeResponse[] quorumResponses = new NodeResponse[quorum];
        int success = 0;
        for (String url : replicas) {
            if (success == quorum) {
                break;
            }

            final Node replica = cluster.getNodeByUrl(url);
            if (replica == null) {
                throw new IllegalStateException("Replica by url is not exist, url: " + url);
            }

            try {
                final NodeResponse response = replica.invoke(request, id, timestamp);

                // not 5xx response
                if (response.getStatus() % HttpURLConnection.HTTP_SERVER_ERROR >= 100) {
                    quorumResponses[success++] = response;
                }
            } catch (Exception e) {
                log.error("Request is failed", e);
            }
        }

        if (success < quorum) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }

        Arrays.sort(quorumResponses, (l, r) -> Long.compare(r.getTimestamp(), l.getTimestamp()));
        return quorumResponses[0];
    }
}
