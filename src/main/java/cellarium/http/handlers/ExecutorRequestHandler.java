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
    private final RequestHandler delegate;

    public ExecutorRequestHandler(ExecutorService executorService, RequestHandler delegate) {
        this.executorService = executorService;
        this.delegate = delegate;
    }

    @Override
    protected void doHandleRequest(Request request, HttpSession session) throws RequestHandlingException {
        try {
            this.executorService.execute(() -> {
                try {
                    delegate.handleRequest(request, session);
                } catch (IOException e) {
                    handleInternalError(session, e);
                }
            });
        } catch (RejectedExecutionException e) {
            try {
                session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
            } catch (IOException ie) {
                throw new RequestHandlingException(ie);
            }
        }
    }
}
