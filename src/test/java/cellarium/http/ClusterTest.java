package cellarium.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.conf.ServerConfig;
import cellarium.http.service.HttpEndpointService;
import one.nio.http.HttpServer;

public class ClusterTest {
    private static String BASE_URL = "http://localhost:";
    private static final Collection<Integer> PORTS = Set.of(8080, 8081, 8082, 8083, 8084);
    private static final List<String> CLUSTER_URLS = PORTS.stream().map(p -> BASE_URL + p).collect(Collectors.toList());

    @Test
    public final void testPutAndGetOnRandomNode() throws IOException, InterruptedException {
        final Set<HttpServer> serverList = new HashSet<>(PORTS.size());

        for (int port : PORTS) {
            serverList.add(
                    new Server(
                            createConfig(port)
                    )
            );
        }

        serverList.forEach(one.nio.server.Server::start);

        int i = ThreadLocalRandom.current().nextInt(0, CLUSTER_URLS.size());
        HttpEndpointService httpEndpointService = new HttpEndpointService(CLUSTER_URLS.get(i) + ServerConfiguration.V_0_ENTITY_ENDPOINT);

        final String id = generateId();
        final byte[] body = generateBody();
        HttpResponse<byte[]> putResponse = httpEndpointService.put(id, body);
        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

        i = ThreadLocalRandom.current().nextInt(0, CLUSTER_URLS.size());
        httpEndpointService = new HttpEndpointService(CLUSTER_URLS.get(i) + ServerConfiguration.V_0_ENTITY_ENDPOINT);

        final HttpResponse<byte[]> getResponse = httpEndpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());

        serverList.forEach(one.nio.server.Server::stop);
    }
}
