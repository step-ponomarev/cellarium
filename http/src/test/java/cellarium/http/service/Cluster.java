package cellarium.http.service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import cellarium.db.DaoConfig;
import cellarium.db.MemorySegmentDao;
import cellarium.http.Server;
import cellarium.http.conf.ConfigNode;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import one.nio.http.HttpServer;
import one.nio.server.AcceptorConfig;

public class Cluster {
    private static final String DIR_PREFIX = "CLUSTER_";

    protected boolean running = true;

    public final Map<String, Set<String>> hostToReplicas;
    private final List<HttpServer> instances;
    private final Map<String, EndpointService> urlToEndpoint;

    protected final Path baseDir;

    public Cluster(Map<String, Set<String>> hostUrlToReplicas, Path baseDir) {
        this(hostUrlToReplicas, baseDir, 1);
    }

    public Cluster(Map<String, Set<String>> hostUrlToReplicas, Path baseDir, int quorum) {
        if (hostUrlToReplicas == null || hostUrlToReplicas.isEmpty()) {
            throw new IllegalArgumentException("Cluster urls cannot be empty");
        }

        if (baseDir == null) {
            throw new NullPointerException("Base dir is null");
        }

        if (Files.notExists(baseDir)) {
            throw new IllegalStateException("Base dir is not exists");
        }

        this.hostToReplicas = hostUrlToReplicas;
        this.instances = new ArrayList<>();
        this.urlToEndpoint = new HashMap<>();
        this.baseDir = baseDir;

        for (String host : hostUrlToReplicas.keySet()) {
            urlToEndpoint.put(host, new SingleEndpointService(host + ServerConfiguration.V_0_ENTITY_ENDPOINT, quorum));
        }
    }

    public void start() throws IOException {
        for (String url : hostToReplicas.keySet()) {
            final URI uri = URI.create(url);
            final Path instanceDir = baseDir.resolve(
                    Path.of(DIR_PREFIX + uri.getPath() + uri.getPort())
            );

            if (Files.notExists(instanceDir)) {
                Files.createDirectory(instanceDir);
            }

            final Server server = new Server(
                    createServerConfig(uri, hostToReplicas),
                    createDao(instanceDir)
            );

            instances.add(server);
            server.start();
        }

        this.running = true;
    }

    public void stop() {
        this.instances.forEach(HttpServer::stop);
        this.instances.clear();

        this.running = false;
    }

    public EndpointService getExactEndpoint(String url) {
        return urlToEndpoint.get(url);
    }

    public EndpointService getRandomEndpoint() {
        Iterator<String> iterator = hostToReplicas.keySet().iterator();

        final int index = ThreadLocalRandom.current().nextInt(0, hostToReplicas.size());
        for (int i = 0; i < index; i++, iterator.next()) {
        }

        return urlToEndpoint.get(iterator.next());
    }

    private static MemorySegmentDao createDao(Path instanceDir) throws IOException {
        final DaoConfig daoConfig = new DaoConfig();
        daoConfig.path = instanceDir.toString();
        daoConfig.memtableTotalSpaceBytes = 1024 * 1024;

        return new MemorySegmentDao(daoConfig);
    }

    private ServerConfig createServerConfig(URI currentUrl, Map<String, Set<String>> clusterUrlToReplicas) {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.selfPort = currentUrl.getPort();
        serverConfig.selfUrl = currentUrl.toString();
        serverConfig.cluster = clusterUrlToReplicas.entrySet().stream().map(u -> {
            final ConfigNode configNode = new ConfigNode();
            configNode.url = u.getKey();
            configNode.replicas = u.getValue();

            return configNode;
        }).collect(Collectors.toSet());
        serverConfig.requestHandlerThreadCount = Runtime.getRuntime().availableProcessors() - 2;

        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = serverConfig.selfPort;
        acceptor.reusePort = true;

        serverConfig.acceptors = new AcceptorConfig[]{
                acceptor
        };

        serverConfig.closeSessions = true;

        return serverConfig;
    }
}
