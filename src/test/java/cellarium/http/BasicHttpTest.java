package cellarium.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class BasicHttpTest extends AHttpTest {
    @Test(timeout = 5000)
    public void testPutSingleEntity() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpEndpointService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = httpEndpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());
    }

    @Test(timeout = 5000)
    public void testGetNonexistentEntity() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpEndpointService.get("fake_id");
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testGetWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpEndpointService.get(null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testDeleteWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpEndpointService.delete(null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testPutWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpEndpointService.put(null, generateBody());
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testPutWithEmptyBody() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpEndpointService.put("fake_id", null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testDeleteEntity() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpEndpointService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = httpEndpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());

        Assert.assertEquals(HttpURLConnection.HTTP_ACCEPTED, httpEndpointService.delete(id).statusCode());
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, httpEndpointService.get(id).statusCode());
    }

    @Test(timeout = 5000)
    public void testReplaceEntry() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] originalBody = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpEndpointService.put(id, originalBody).statusCode());

        HttpResponse<byte[]> getResponse = httpEndpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(originalBody, getResponse.body());

        final byte[] newBody = generateBody();
        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpEndpointService.put(id, newBody).statusCode());

        getResponse = httpEndpointService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(newBody, getResponse.body());
    }
}
