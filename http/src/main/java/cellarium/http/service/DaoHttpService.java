package cellarium.http.service;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.db.dao.CellariumDiskStorage;
import cellarium.db.utils.MemorySegmentUtils;
import jdk.incubator.foreign.MemorySegment;
import one.nio.http.Response;

public final class DaoHttpService implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DaoHttpService.class);

    private final CellariumDiskStorage dao;

    public DaoHttpService(CellariumDiskStorage dao) {
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
                    MemorySegmentUtils.stringToMemorySegment(id)
            );

            if (entry == null) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }

            return Response.ok(
                    entry.getValue().toByteArray()
            );
        } catch (Exception e) {
            log.error("Get by id is failed, id: " + id, e);
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

            dao.put(
                    new MemorySegmentEntry(
                            MemorySegmentUtils.stringToMemorySegment(id),
                            MemorySegment.ofArray(body),
                            System.currentTimeMillis()
                    )
            );

            return new Response(Response.CREATED, Response.EMPTY);
        } catch (Exception e) {
            log.error("Put is failed, id: " + id, e);
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
    }

    public Response delete(String id) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            dao.put(
                    new MemorySegmentEntry(
                            MemorySegmentUtils.stringToMemorySegment(id),
                            null,
                            System.currentTimeMillis()
                    )
            );

            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (Exception e) {
            log.error("Remove is failed, id: " + id, e);
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
    }
}

