package cellarium.http.service;


import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class HttpService {
    private final static HttpClient client = HttpClient.newHttpClient();

    private final String endpoint;

    public HttpService(String endpoint) {
        this.endpoint = endpoint;
    }

    public HttpResponse<byte[]> put(String id, byte[] body) throws IOException, InterruptedException {
        return client.send(
                createRequestById(id).PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    public HttpResponse<byte[]> get(String id) throws IOException, InterruptedException {
        return client.send(
                createRequestById(id).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private HttpRequest.Builder createRequestById(String id) {
        return createRequest("?id=" + id);
    }

    private HttpRequest.Builder createRequest(String path) {
        return HttpRequest.newBuilder(URI.create(endpoint + path));
    }
}
