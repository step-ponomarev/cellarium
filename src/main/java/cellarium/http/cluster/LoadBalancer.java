package cellarium.http.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public final class LoadBalancer implements Closeable {
    private final Map<String, BlockingDeque<CancelableTask>> nodeUrlToTasks;
    private final ExecutorService executorService;

    public LoadBalancer(Set<String> clusterUrls, int threadCount, int executingTaskPerNode) {
        this.nodeUrlToTasks = new HashMap<>();
        this.executorService = Executors.newFixedThreadPool(threadCount);

        for (String url : clusterUrls) {
            this.nodeUrlToTasks.put(url, new LinkedBlockingDeque<>(executingTaskPerNode));
        }
    }

    public void scheduleTask(String url, Runnable task, Consumer<Throwable> onError) {
        final BlockingDeque<CancelableTask> nodeTasks = nodeUrlToTasks.get(url);

        boolean threadLimitIsReached = false;
        final CancelableTask cancelableTask = new CancelableTask(task, onError);

        while (!nodeTasks.offer(cancelableTask)) {
            threadLimitIsReached = true;
            final CancelableTask currentTask = nodeTasks.poll();
            if (currentTask != null) {
                currentTask.cancel();
            }
        }

        if (threadLimitIsReached) {
            return;
        }

        try {
            CompletableFuture.runAsync(() -> {
                        final CancelableTask currentTask = nodeTasks.poll();
                        if (currentTask == null) {
                            return;
                        }

                        currentTask.run();
                    }, executorService)
                    .exceptionallyAsync(e -> {
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
