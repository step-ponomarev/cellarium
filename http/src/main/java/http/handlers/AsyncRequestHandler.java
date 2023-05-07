package http.handlers;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;

public abstract class AsyncRequestHandler implements RequestHandler, Closeable {
    private static final Logger log = LoggerFactory.getLogger(AsyncRequestHandler.class);

    private final Object executorLock = new Object();
    private final ExecutorService executorService;

    protected AsyncRequestHandler(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public final void handleRequest(Request request, HttpSession session) {
        try {
            synchronized (executorLock) {
                handleRequestAsync(request, session, executorService);
            }
        } catch (RejectedExecutionException e) {
            sendErrorResponse(session, e);
        }
    }

    @Override
    public final void close() throws IOException {
        synchronized (executorLock) {
            executorService.shutdown();
        }

        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        handleClose();
    }

    protected abstract void handleRequestAsync(Request request, HttpSession session, ExecutorService executorService) throws RejectedExecutionException;

    protected void handleClose() throws IOException {}

    protected static void sendErrorResponse(HttpSession session, Throwable e) {
        try {
            log.error("Response is failed ", e);
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY));
        } catch (IOException ie) {
            log.error("Closing session", ie);
            session.close();
        }
    }
}
