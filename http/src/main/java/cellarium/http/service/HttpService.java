package cellarium.http.service;

import one.nio.http.Request;
import one.nio.http.Response;

public interface HttpService {
    Response handleRequest(Request request);
}
