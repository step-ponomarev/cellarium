package cellarium.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.db.MemorySegmentDao;
import cellarium.http.cluster.Cluster;
import cellarium.http.cluster.ConsistentHashing;
import cellarium.http.cluster.LoadBalancer;
import cellarium.http.cluster.Node;
import cellarium.http.cluster.NodeResponse;
import cellarium.http.cluster.request.LocalRequestHandler;
import cellarium.http.cluster.request.NodeRequest;
import cellarium.http.cluster.request.RemoteRequestHandler;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.Hash;

public final class Server extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final Cluster cluster;
    private final LoadBalancer loadBalancer;
    private final LocalRequestHandler localRequestHandler;

    public Server(ServerConfig config, MemorySegmentDao dao) throws IOException {
        super(config);

        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        final Map<String, Set<String>> urlToReplicas = config.cluster.stream()
                .collect(Collectors.toMap(u -> u.url, u -> u.replicas));

        this.localRequestHandler = new LocalRequestHandler(dao);
        this.cluster = createCluster(config.selfUrl, urlToReplicas, config.virtualNodeAmount, localRequestHandler);
        this.loadBalancer = new LoadBalancer(urlToReplicas.keySet(), config.requestHandlerThreadCount, config.maxTasksPerNode);
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isValidRequest(request)) {
            handleDefault(request, session);
            return;
        }

        final Map<String, String> reqeustParams = new HashMap<>();
        for (Map.Entry<String, String> param : request.getParameters()) {
            reqeustParams.put(param.getKey(), param.getValue());
        }

        final String id = reqeustParams.get(QueryParam.ID);
        if (id == null) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        final boolean internalClusterRequest = reqeustParams.containsKey(QueryParam.TIMESTAMP);
        final long timestamp = internalClusterRequest
                ? Long.parseLong(reqeustParams.get(QueryParam.TIMESTAMP))
                : System.currentTimeMillis();

        final String quorumStr = reqeustParams.get(QueryParam.QUORUM);
        final int quorum = quorumStr == null ? 1 : Integer.parseInt(quorumStr);

        final Node candidateNode = cluster.getNodeByIndex(
                ConsistentHashing.getNodeIndexForHash(Hash.murmur3(id), cluster.getVirtualNodeAmount())
        );

        final Runnable task = () -> {
            try {
                final NodeRequest nodeRequest = NodeRequest.of(request, id, timestamp);
                if (internalClusterRequest || quorum == 1) {
                    session.sendResponse(
                            candidateNode.invoke(nodeRequest)
                    );
                    return;
                }

                final Set<String> replicas = cluster.getReplicaUrlsByUrl(candidateNode.getNodeUrl());
                session.sendResponse(
                        handleDistributedReqeust(nodeRequest, replicas, quorum)
                );
            } catch (Exception e) {
                sendErrorResponse(session, e);
            }
        };

        try {
            final boolean scheduled = loadBalancer.scheduleTask(
                    candidateNode.getNodeUrl(),
                    task
            );

            if (!scheduled) {
                session.sendResponse(
                        new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY)
                );
            }
        } catch (RejectedExecutionException e) {
            sendErrorResponse(session, e);
        }
    }

    private Response handleDistributedReqeust(NodeRequest request, Set<String> replicaUrls, int quorum) {
        final String[] replicas = loadBalancer.sortByLoadFactor(replicaUrls);
        if (replicas.length < quorum) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }

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
                final NodeResponse response = replica.invoke(request);
                if (response.getStatus() != HttpURLConnection.HTTP_UNAVAILABLE) {
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
            localRequestHandler.close();
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

    private static Cluster createCluster(String selfUrl,
                                         Map<String, Set<String>> urlToReplicas,
                                         int virtualNodeAmount,
                                         LocalRequestHandler localRequestHandler) {
        final Map<String, Node> urlToNode = urlToReplicas.keySet().stream()
                .collect(Collectors.toMap(
                        UnaryOperator.identity(),
                        url -> new Node(url, url.equals(selfUrl) ? localRequestHandler : new RemoteRequestHandler(url))
                ));

        final String[] nodeUrls = urlToReplicas.keySet().toArray(String[]::new);
        final Node[] virtualNodes = new Node[nodeUrls.length * virtualNodeAmount];
        // node order for 3 nodes and 2 virual for each: [1, 2, 3, 1, 2, 3]
        for (int i = 0; i < virtualNodes.length; i++) {
            virtualNodes[i] = urlToNode.get(
                    nodeUrls[i % nodeUrls.length]
            );
        }

        final Map<String, Set<Node>> urlToReplicaNodes = new HashMap<>();
        for (String url : urlToNode.keySet()) {
            final Set<String> replicaUrls = urlToReplicas.get(url);
            final Node[] replicas = new Node[replicaUrls.size() + 1];

            int i = 0;
            for (String replicaUrl : replicaUrls) {
                replicas[i++] = urlToNode.get(replicaUrl);
            }
            replicas[i] = urlToNode.get(url);

            urlToReplicaNodes.put(url, Set.of(replicas));
        }

        return new Cluster(virtualNodes, urlToReplicaNodes);
    }
}
