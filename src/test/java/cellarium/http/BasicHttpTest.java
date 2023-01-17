package cellarium.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.Collections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.conf.ServiceConfig;
import cellarium.http.service.HttpService;
import one.nio.http.HttpServer;

public class BasicHttpTest extends AHttpTest {
    private final HttpServer server;

    private static final int BODY_LEN_BYTES = 40;

    private static final int PORT = 8080;
    private static final String URL = "http://localhost:" + PORT;
    private static final String ENDPOINT = URL + ServerConfiguration.V_0_ENTITY_ENDPOINT;

    public BasicHttpTest() throws IOException {
        final ServiceConfig config = new ServiceConfig(
                PORT,
                URL,
                Collections.singletonList(URL),
                Files.createTempDirectory("TMP_DIR")
        );

        this.server = new Server(config);
    }

    @Before
    public void startServer() {
        this.server.start();
    }

    @After
    public void stopServer() {
        this.server.stop();
    }

    @Test
    public void testPutSingleEntity() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody(BODY_LEN_BYTES);

        final HttpService httpService = new HttpService(ENDPOINT);
        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = httpService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());
    }
}
