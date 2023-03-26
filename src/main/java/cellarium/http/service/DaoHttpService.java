package cellarium.http.service;

import java.io.Closeable;
import java.io.IOException;
import cellarium.dao.MemorySegmentDao;
import cellarium.dao.entry.MemorySegmentEntry;
import cellarium.dao.utils.Utils;
import cellarium.http.QueryParam;
import jdk.incubator.foreign.MemorySegment;
import one.nio.http.Request;
import one.nio.http.Response;

public final class DaoHttpService implements HttpService, Closeable {
    private final MemorySegmentDao dao;

    public DaoHttpService(MemorySegmentDao dao) {
        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.dao = dao;
    }

    @Override
    public Response handleRequest(Request request) {
        return switch (request.getMethod()) {
            case Request.METHOD_GET -> getById(request);
            case Request.METHOD_PUT -> put(request);
            case Request.METHOD_DELETE -> delete(request);
            default -> new Response(Response.BAD_REQUEST);
        };
    }

    @Override
    public void close() throws IOException {
        dao.close();
    }

    private Response getById(Request request) {
        final String id = request.getParameter(QueryParam.ID);
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            final MemorySegmentEntry entry = dao.get(
                    Utils.stringToMemorySegment(id)
            );
            if (entry == null) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }

            return Response.ok(
                    entry.getValue().toByteArray()
            );
        } catch (Exception e) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
    }

    private Response put(Request request) {
        final String id = request.getParameter(QueryParam.ID);
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            final byte[] body = request.getBody();
            if (body == null || body.length == 0) {
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
            }

            dao.upsert(
                    new MemorySegmentEntry(
                            Utils.stringToMemorySegment(id),
                            MemorySegment.ofArray(body),
                            System.currentTimeMillis()
                    )
            );

            return new Response(Response.CREATED, Response.EMPTY);
        } catch (Exception e) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
    }

    private Response delete(Request request) {
        final String id = request.getParameter(QueryParam.ID);
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            dao.upsert(
                    new MemorySegmentEntry(
                            Utils.stringToMemorySegment(id),
                            null,
                            System.currentTimeMillis()
                    )
            );

            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (Exception e) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
    }
}

