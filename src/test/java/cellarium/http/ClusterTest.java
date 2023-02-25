package cellarium.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import cellarium.http.service.HttpEndpointService;

public class ClusterTest extends AHttpTest {
    private static String BASE_URL = "http://localhost:";
    private static final Set<Integer> PORTS = Set.of(8080, 8081, 8082, 8083, 8084);
    private static final List<String> CLUSTER_URLS = PORTS.stream().map(p -> BASE_URL + p).toList();

    public ClusterTest() {
        super(PORTS);
    }

    @Test
    public final void testPutAndGetFromDifferentNodes() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody();

        HttpEndpointService service = getRandomHostEndpointService();
        HttpResponse<byte[]> putResponse = service.put(id, body);
        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

        HttpEndpointService otherEndpoint = getRandomHostEndpointService();
        while (otherEndpoint.equals(service)) {
            otherEndpoint = getRandomHostEndpointService();
        }

        final HttpResponse<byte[]> getResponse = otherEndpoint.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());
    }
}
