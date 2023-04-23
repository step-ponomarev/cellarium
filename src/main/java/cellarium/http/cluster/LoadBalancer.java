package cellarium.http.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoadBalancer implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);

    private final Map<String, BlockingQueue<CancelableTask>> nodeUrlToTasks;
    private final ExecutorService executorService;

    public LoadBalancer(Set<String> clusterUrls, int threadCount, int executingTaskPerNode) {
        this.nodeUrlToTasks = new HashMap<>();
        this.executorService = Executors.newFixedThreadPool(threadCount);

        for (String url : clusterUrls) {
            this.nodeUrlToTasks.put(url, new LinkedBlockingQueue<>(executingTaskPerNode));
        }
    }

    public void scheduleTask(String url, Runnable task, Consumer<Throwable> onError) {
        final BlockingQueue<CancelableTask> nodeTasks = nodeUrlToTasks.get(url);

        final CancelableTask cancelableTask = new CancelableTask(task, onError);
        if (!nodeTasks.offer(cancelableTask)) {
            cancelableTask.cancel();
            return;
        }

        try {
            CompletableFuture.runAsync(() -> nodeTasks.poll().run(), executorService)
                    .exceptionallyAsync(e -> {
                        log.error("Request is failed", e);
                        onError.accept(e);
                        return null;
                    }, executorService);
        } catch (RejectedExecutionException e) {
            onError.accept(e);
        }
    }

    private static final class CancelableTask implements Runnable {
        private final Runnable task;
        private final Consumer<Throwable> onCancel;

        public CancelableTask(Runnable task, Consumer<Throwable> onCancel) {
            this.task = task;
            this.onCancel = onCancel;
        }

        @Override
        public void run() {
            this.task.run();
        }

        public void cancel() {
            onCancel.accept(new TimeoutException("Canceled"));
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Error occurred while shutting down ExecutorService", e);
        }
    }
}
