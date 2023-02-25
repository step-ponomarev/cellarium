package cellarium.http.service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import cellarium.disk.DiskUtils;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import one.nio.http.HttpServer;

public class Cluster {
    private static final String DIR_PREFIX = "CLUSTER_";

    protected boolean running = true;

    public final List<String> clusterUrls;
    private final List<HttpServer> instances;
    private final Map<String, EndpointService> urlToEndpoint;
    private final Path baseDir;

    public Cluster(List<String> clusterUrls, Path baseDir) throws IOException {
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
                    createServerConfig(uri, clusterUrls, instanceDir)
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

    public void clearData() throws IOException {
        DiskUtils.removeDir(this.baseDir);
    }

    public EndpointService getExactEndpoint(String url) {
        return urlToEndpoint.get(url);
    }

    public EndpointService getRandomEndpoint() {
        final String url = clusterUrls.get(ThreadLocalRandom.current().nextInt(0, clusterUrls.size()));

        return urlToEndpoint.get(url);
    }

    private static ServerConfig createServerConfig(URI currentUrl, List<String> clusterUrls, Path instanceDir) throws IOException {
        return new ServerConfig(
                currentUrl.getPort(),
                currentUrl.toString(),
                clusterUrls,
                instanceDir,
                Math.max(1, clusterUrls.size() / (Runtime.getRuntime().availableProcessors() - 2))
        );
    }
}