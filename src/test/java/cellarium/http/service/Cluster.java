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
import cellarium.dao.DaoConfig;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import one.nio.http.HttpServer;
import one.nio.server.AcceptorConfig;

public class Cluster {
    private static final String DIR_PREFIX = "CLUSTER_";

    protected boolean running = true;

    public final Set<String> clusterUrls;
    private final List<HttpServer> instances;
    private final Map<String, EndpointService> urlToEndpoint;
    protected final Path baseDir;

    public Cluster(Set<String> clusterUrls, Path baseDir) {
        if (clusterUrls == null || clusterUrls.isEmpty()) {
            throw new IllegalArgumentException("Cluster urls cannot be empty");
        }

        if (baseDir == null) {
            throw new NullPointerException("Base dir is null");
        }

        if (Files.notExists(baseDir)) {
            throw new IllegalStateException("Base dir is not exists");
        }

        this.clusterUrls = clusterUrls;
        this.instances = new ArrayList<>();
        this.urlToEndpoint = new HashMap<>();
        this.baseDir = baseDir;

        for (String url : clusterUrls) {
            urlToEndpoint.put(url, new SingleEndpointService(url + ServerConfiguration.V_0_ENTITY_ENDPOINT));
        }
    }

    public void start() throws IOException {
        for (String url : clusterUrls) {
            final URI uri = URI.create(url);
            final Path instanceDir = baseDir.resolve(
                    Path.of(DIR_PREFIX + uri.getPath() + uri.getPort())
            );

            if (Files.notExists(instanceDir)) {
                Files.createDirectory(instanceDir);
            }

            final cellarium.http.Server server = new cellarium.http.Server(
                    createServerConfig(uri, clusterUrls),
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
        Iterator<String> iterator = clusterUrls.iterator();

        final int index = ThreadLocalRandom.current().nextInt(0, clusterUrls.size());
        for (int i = 0; i < index; i++, iterator.next()) {}

        return urlToEndpoint.get(iterator.next());
    }

    private static MemorySegmentDao createDao(Path instanceDir) throws IOException {
        final DaoConfig daoConfig = new DaoConfig();
        daoConfig.path = instanceDir.toString();
        daoConfig.memtableLimitBytes = 1024 * 1024;

        return new MemorySegmentDao(daoConfig);
    }

    private static ServerConfig createServerConfig(URI currentUrl, Set<String> clusterUrls) {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.selfPort = currentUrl.getPort();
        serverConfig.selfUrl = currentUrl.toString();
        serverConfig.clusterUrls = clusterUrls;
        serverConfig.threadCount = Runtime.getRuntime().availableProcessors() - 2;

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
