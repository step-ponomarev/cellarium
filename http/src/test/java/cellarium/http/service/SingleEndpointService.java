package cellarium.http.service;


import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import cellarium.http.QueryParam;

public final class SingleEndpointService implements EndpointService {
    private final static HttpClient client = HttpClient.newHttpClient();
    private final int quorum;

    private final String endpoint;
    
    public SingleEndpointService(String endpoint, int quorum) {
        this.endpoint = endpoint;
        this.quorum = quorum;
    }

    @Override
    public HttpResponse<byte[]> put(String id, byte[] body) throws IOException, InterruptedException {
        final HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body);

        return client.send(
                createRequestById(id).PUT(publisher).build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    @Override
    public HttpResponse<byte[]> get(String id) throws IOException, InterruptedException {
        return client.send(
                createRequestById(id).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    @Override
    public HttpResponse<byte[]> delete(String id) throws IOException, InterruptedException {
        return client.send(
                createRequestById(id).DELETE().build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private HttpRequest.Builder createRequestById(String id) {
        if (id == null) {
            return createRequest("");
        }

        return createRequest("?" + QueryParam.ID + "=" + id
                + "&" + QueryParam.QUORUM + "=" + quorum
        );
    }

    private HttpRequest.Builder createRequest(String path) {
        return HttpRequest.newBuilder(URI.create(endpoint + path));
    }
}
