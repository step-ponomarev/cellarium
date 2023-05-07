package cellarium.http.cluster.request;

import java.io.Closeable;
import java.io.IOException;
import cellarium.http.service.DaoHttpService;
import one.nio.http.Request;
import one.nio.http.Response;

public final class LocalRequestHandler implements NodeRequestHandler, Closeable {
    private final DaoHttpService daoHttpService;

    public LocalRequestHandler(DaoHttpService daoHttpService) {
        this.daoHttpService = daoHttpService;
    }

    @Override
    public Response handleReqeust(NodeRequest request) {
        return switch (request.getMethod()) {
            case Request.METHOD_GET -> daoHttpService.getById(request.getId());
            case Request.METHOD_PUT -> daoHttpService.put(request.getId(), request.getBody());
            case Request.METHOD_DELETE -> daoHttpService.delete(request.getId());
            default -> new Response(Response.BAD_REQUEST);
        };
    }

    @Override
    public void close() throws IOException {
        daoHttpService.close();
    }
}
