package cellarium.http.handlers;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public abstract class ARequestHandler implements RequestHandler {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public final void handleRequest(Request request, HttpSession session) {
        try {
            doHandleRequest(request, session);
        } catch (RequestHandlingException e) {
            handleInternalError(session, e);
        }
    }

    protected abstract void doHandleRequest(Request request, HttpSession session) throws RequestHandlingException;

    protected final void handleInternalError(HttpSession session, Exception e) {
        try {
            log.error("Cannot handle request", e);
            session.sendResponse(new Response(Response.INTERNAL_ERROR));
        } catch (IOException ex) {
            log.error("Response is failed, closing socket", ex);
            session.socket().close();
        }
    }
}
