package cellarium.dao;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ThreadSafeExecutor {
    private final ExecutorService executorService;

    public ThreadSafeExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public synchronized void execute(Runnable task) {
        this.executorService.execute(task);
    }
    
    public synchronized void close(long timeout) {
        executorService.shutdown();

        try {
            executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
