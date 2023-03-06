package cellarium.http.service;

import java.io.IOException;
import java.net.http.HttpResponse;

public interface EndpointService {
    HttpResponse<byte[]> put(String id, byte[] body) throws IOException, InterruptedException;

    HttpResponse<byte[]> get(String id) throws IOException, InterruptedException;

    HttpResponse<byte[]> delete(String id) throws IOException, InterruptedException;
}
