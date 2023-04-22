package cellarium.http.service;

import java.io.Closeable;
import java.io.IOException;
import cellarium.dao.MemorySegmentDao;
import cellarium.dao.entry.MemorySegmentEntry;
import cellarium.dao.utils.Utils;
import jdk.incubator.foreign.MemorySegment;
import one.nio.http.Response;

public final class DaoHttpService implements Closeable {
    private final MemorySegmentDao dao;

    public DaoHttpService(MemorySegmentDao dao) {
        if (dao == null) {
            throw new NullPointerException("Dao cannot be null");
        }

        this.dao = dao;
    }

    @Override
    public void close() throws IOException {
        dao.close();
    }

    public Response getById(String id) {
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

    public Response put(String id, byte[] body) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
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

    public Response delete(String id) {
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

