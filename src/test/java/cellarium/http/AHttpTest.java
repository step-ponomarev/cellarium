package cellarium.http;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import cellarium.http.conf.ServerConfig;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.service.HttpEndpointService;
import one.nio.http.HttpServer;

public abstract class AHttpTest {
    private static final String DIR_PREFIX = "TMP_";
    private static final int DEFAULT_PORT = 8080;
    private static final int BODY_LEN_BYTES = 40;
    private static final String URL = "http://localhost:";

    private final Set<Integer> ports;
    private final List<String> clusterUrls;
    
    private Set<HttpServer> servers;
    private Map<String, HttpEndpointService> urlToEndpoint;

    public AHttpTest() {
        this(Set.of(DEFAULT_PORT));
    }

    public AHttpTest(Set<Integer> ports) {
        this.ports = ports;
        this.clusterUrls = this.ports.stream().map(p -> URL + p).collect(Collectors.toList());
    }

    @Before
    public void start() throws IOException {
        final Set<HttpServer> servers = new HashSet<>();
        final Map<String, HttpEndpointService> urlToEndpoint = new HashMap<>();

        for (int port : ports) {
            final ServerConfig config = createConfig(port);
            servers.add(new Server(config));

            final String url = URL + port;
            urlToEndpoint.put(url, new HttpEndpointService(url + ServerConfiguration.V_0_ENTITY_ENDPOINT));
        }

        this.servers = servers;
    }

    @After
    public void stop() {
        servers.stop();
        servers = null;
    }

    protected static byte[] generateBody() {
        return generateRandomBytes(BODY_LEN_BYTES);
    }

    protected static String generateId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    private static byte[] generateRandomBytes(int len) {
        if (len <= 0) {
            throw new IllegalArgumentException("Len should be more than 0");
        }

        byte[] bytes = new byte[len];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private ServerConfig createConfig(int currentPort) throws IOException {
        return new ServerConfig(
                currentPort,
                URL + currentPort,
                this.clusterUrls,
                Files.createTempDirectory(DIR_PREFIX + currentPort),
                ports.size() / (Runtime.getRuntime().availableProcessors() - 2)
        );
    }
}
