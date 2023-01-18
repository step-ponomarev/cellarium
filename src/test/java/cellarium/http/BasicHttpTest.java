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

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = httpService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());
    }

    @Test(timeout = 5000)
    public void testGetNonexistentEntity() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpService.get("fake_id");
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testGetWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpService.get(null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testDeleteWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpService.delete(null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testPutWithoutId() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpService.put(null, generateBody());
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testPutWithEmptyBody() throws IOException, InterruptedException {
        final HttpResponse<byte[]> getResponse = httpService.put("fake_id", null);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, getResponse.statusCode());
    }

    @Test(timeout = 5000)
    public void testDeleteEntity() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] body = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpService.put(id, body).statusCode());

        final HttpResponse<byte[]> getResponse = httpService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(body, getResponse.body());

        Assert.assertEquals(HttpURLConnection.HTTP_ACCEPTED, httpService.delete(id).statusCode());
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, httpService.get(id).statusCode());
    }

    @Test(timeout = 5000)
    public void testReplaceEntry() throws IOException, InterruptedException {
        final String id = generateId();
        final byte[] originalBody = generateBody();

        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpService.put(id, originalBody).statusCode());

        HttpResponse<byte[]> getResponse = httpService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(originalBody, getResponse.body());

        final byte[] newBody = generateBody();
        Assert.assertEquals(HttpURLConnection.HTTP_CREATED, httpService.put(id, newBody).statusCode());

        getResponse = httpService.get(id);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        Assert.assertArrayEquals(newBody, getResponse.body());
    }
}
