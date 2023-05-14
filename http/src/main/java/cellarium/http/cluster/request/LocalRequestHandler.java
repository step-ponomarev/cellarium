package cellarium.http.cluster.request;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.db.MemorySegmentDao;
import cellarium.db.entry.MemorySegmentEntry;
import cellarium.db.utils.MemorySegmentUtils;
import cellarium.http.cluster.NodeResponse;
import jdk.incubator.foreign.MemorySegment;
import one.nio.http.Request;
import one.nio.http.Response;

public final class LocalRequestHandler implements NodeRequestHandler, Closeable {
    private static final Logger log = LoggerFactory.getLogger(LocalRequestHandler.class);

    private final MemorySegmentDao dao;

    public LocalRequestHandler(MemorySegmentDao dao) {
        this.dao = dao;
    }

    @Override
    public NodeResponse handleReqeust(NodeRequest request) {
        return switch (request.getMethod()) {
            case Request.METHOD_GET -> handleGet(request.getId());
            case Request.METHOD_PUT -> handlePut(request.getId(), request.getBody(), request.getTimestamp());
            case Request.METHOD_DELETE -> handleDelete(request.getId(), request.getTimestamp());
            default -> new NodeResponse(Response.BAD_REQUEST, Response.EMPTY, System.currentTimeMillis());
        };
    }

    private NodeResponse handleGet(String id) {
        final long currentTimeMillis = System.currentTimeMillis();
        if (id == null || id.isEmpty()) {
            return new NodeResponse(Response.BAD_REQUEST, Response.EMPTY, currentTimeMillis);
        }

        try {
            final MemorySegmentEntry entry = dao.get(
                    MemorySegmentUtils.stringToMemorySegment(id)
            );

            if (entry == null) {
                return new NodeResponse(Response.NOT_FOUND, Response.EMPTY, currentTimeMillis);
            }

            return new NodeResponse(
                    Response.ok(
                            entry.getValue().toByteArray()
                    ),
                    entry.getTimestamp()
            );
        } catch (Exception e) {
            log.error("Get by id is failed, id: " + id, e);
            return new NodeResponse(Response.SERVICE_UNAVAILABLE, Response.EMPTY, currentTimeMillis);
        }
    }

    private NodeResponse handlePut(String id, byte[] body, long reqeustTimestamp) {
        final long currentTimeMillis = System.currentTimeMillis();

        if (id == null || id.isEmpty()) {
            return new NodeResponse(Response.BAD_REQUEST, Response.EMPTY, currentTimeMillis);
        }

        try {
            if (body == null || body.length == 0) {
                return new NodeResponse(Response.BAD_REQUEST, Response.EMPTY, currentTimeMillis);
            }

            dao.upsert(
                    new MemorySegmentEntry(
                            MemorySegmentUtils.stringToMemorySegment(id),
                            MemorySegment.ofArray(body),
                            reqeustTimestamp
                    )
            );

            return new NodeResponse(Response.CREATED, Response.EMPTY, currentTimeMillis);
        } catch (Exception e) {
            log.error("Put is failed, id: " + id, e);
            return new NodeResponse(Response.SERVICE_UNAVAILABLE, Response.EMPTY, currentTimeMillis);
        }
    }

    private NodeResponse handleDelete(String id, long reqeustTimestamp) {
        final long currentTimeMillis = System.currentTimeMillis();

        if (id == null || id.isEmpty()) {
            return new NodeResponse(Response.BAD_REQUEST, Response.EMPTY, currentTimeMillis);
        }

        try {
            dao.upsert(
                    new MemorySegmentEntry(
                            MemorySegmentUtils.stringToMemorySegment(id),
                            null,
                            reqeustTimestamp
                    )
            );

            return new NodeResponse(Response.ACCEPTED, Response.EMPTY, currentTimeMillis);
        } catch (Exception e) {
            log.error("Remove is failed, id: " + id, e);
            return new NodeResponse(Response.SERVICE_UNAVAILABLE, Response.EMPTY, currentTimeMillis);
        }
    }

    @Override
    public void close() throws IOException {
        dao.close();
    }
}
