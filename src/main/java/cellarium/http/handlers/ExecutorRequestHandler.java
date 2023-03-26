package cellarium.http.handlers;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public final class ExecutorRequestHandler extends ARequestHandler {
    private final ExecutorService executorService;
    private final RequestHandler handler;

    public ExecutorRequestHandler(ExecutorService executorService, RequestHandler handler) {
        this.executorService = executorService;
        this.handler = handler;
    }

    @Override
    protected void doHandleRequest(Request request, HttpSession session) throws RequestHandlingException {
        try {
            this.executorService.execute(() -> delegateRequest(request, session));
        } catch (RejectedExecutionException e) {
            handleRejectedExecutionException(session);
        }
    }

    private void delegateRequest(Request request, HttpSession session) {
        try {
            handler.handleRequest(request, session);
        } catch (IOException e) {
            handleInternalError(session, e);
        }
    }

    private void handleRejectedExecutionException(HttpSession session) throws RequestHandlingException {
        try {
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
        } catch (IOException ie) {
            throw new RequestHandlingException(ie);
        }
    }
}
