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
import cellarium.DiskUtils;
import cellarium.http.service.Cluster;
import cellarium.http.service.EndpointService;

public class BasicHttpTest extends AHttpTest {
    private final static String URL = "http://localhost:8080";

    private Cluster cluster;
    private EndpointService endpointService;

    @Before
    public void beforeEachTest() throws IOException {
        if (Files.exists(DEFAULT_DIR)) {
            DiskUtils.removeDir(DEFAULT_DIR);
        }

        Files.createDirectory(DEFAULT_DIR);

        cluster = new Cluster(Collections.singleton(URL), DEFAULT_DIR);
        cluster.start();

        endpointService = cluster.getRandomEndpoint();
    }

    @After
    public void afterEachTest() throws IOException {
        cluster.stop();
        endpointService = null;

        if (Files.exists(DEFAULT_DIR)) {
            DiskUtils.removeDir(DEFAULT_DIR);
        }
    }

    @Test(timeout = 5000)
    public void testPutSingleEntity() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, endpointService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = endpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());
    }

    @Test(timeout = 5000)
    public void testGetNonexistentEntity() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = endpointService.get("fake_id");
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testGetWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = endpointService.get(null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testDeleteWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = endpointService.delete(null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testPutWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = endpointService.put(null, generateBody());
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testPutWithEmptyBody() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = endpointService.put("fake_id", null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testDeleteEntity() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, endpointService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = endpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());

        Assert.assertEquals(HttpURLConnection.HTTP_ACCEPTED, endpointService.delete(id).statusCode());
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, endpointService.get(id).statusCode());
    }

    @Test(timeout = 5000)
    public void testReplaceEntry() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] originalBody = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, endpointService.put(id, originalBody).statusCode());

        HttpResponse<byte[]> getResponse = endpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(originalBody, getResponse.body());

        final byte[] newBody = generateBody();
        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, endpointService.put(id, newBody).statusCode());

        getResponse = endpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(newBody, getResponse.body());
    }
}
