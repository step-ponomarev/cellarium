package cellarium.http.handlers;

import cellarium.dao.MemorySegmentDao;
import cellarium.entry.MemorySegmentEntry;
import cellarium.http.conf.ServerConfiguration;
import cellarium.utils.Utils;
import jdk.incubator.foreign.MemorySegment;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.http.VirtualHost;

@VirtualHost(EntityRequestHandler.ROUTER_NAME)
public class EntityRequestHandler implements RequestHandler {
    public static final String ROUTER_NAME = "ENTITY";

    private final MemorySegmentDao dao;

    public EntityRequestHandler(MemorySegmentDao dao) {
        this.dao = dao;
    }

    @RequestMethod(Request.METHOD_GET)
    @Path(ServerConfiguration.V_0_ENTITY_ENDPOINT)
    public Response getById(@Param(value = "id", required = true) String id) {
        if (id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            final MemorySegmentEntry entry = dao.get(
                    Utils.stringToMemorySegment(id)
            );
            if (entry == null) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }

            //TODO: Все ли тут ок?
            return Response.ok(
                    entry.getValue().asByteBuffer().array()
            );
        } catch (Exception e) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
    }

    @RequestMethod(Request.METHOD_PUT)
    @Path(ServerConfiguration.V_0_ENTITY_ENDPOINT)
    public Response put(Request request, @Param(value = "id", required = true) String id) {
        if (id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            final byte[] body = request.getBody();
            if (body == null) {
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

    @RequestMethod(Request.METHOD_DELETE)
    @Path(ServerConfiguration.V_0_ENTITY_ENDPOINT)
    public Response delete(@Param(value = "id", required = true) String id) {
        if (id.isEmpty()) {
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

    @Override
    public void handleRequest(Request request, HttpSession session) {}
}

