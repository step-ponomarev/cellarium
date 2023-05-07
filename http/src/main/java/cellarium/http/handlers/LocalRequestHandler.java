package cellarium.http.handlers;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import cellarium.http.service.DaoHttpService;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;

public final class LocalRequestHandler extends AsyncRequestHandler {
    private final DaoHttpService daoHttpService;

    public LocalRequestHandler(DaoHttpService daoHttpService, int threadCount) {
        super(Executors.newFixedThreadPool(threadCount));
        if (threadCount < 1) {
            throw new IllegalArgumentException("Invalid thread count: " + threadCount);
        }

        this.daoHttpService = daoHttpService;
    }

    @Override
    protected void handleRequestAsync(Request request, HttpSession session, ExecutorService executorService) throws RejectedExecutionException {
        executorService.execute(() -> {
            final Response response = daoHttpService.handleRequest(request);
            try {
                session.sendResponse(response);
            } catch (IOException e) {
                sendErrorResponse(session, e);
            }
        });
    }

    @Override
    protected void handleClose() throws IOException {
        daoHttpService.close();
    }
}
