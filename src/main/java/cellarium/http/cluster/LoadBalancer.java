package cellarium.http.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class LoadBalancer implements Closeable {
    private final Map<String, BlockingQueue<CancelableTask>> nodeUrlToTasks;
    private final ExecutorService executorService;

    public static final class CancelableTask implements Runnable {
        private final Runnable task;
        private final Consumer<Throwable> onCancel;
        private volatile boolean canceled = false;

        public CancelableTask(Runnable task, Consumer<Throwable> onCancel) {
            this.task = task;
            this.onCancel = onCancel;
        }

        @Override
        public synchronized void run() {
            if (canceled) {
                throw new CancellationException("Canceled");
            }

            task.run();
        }

        public synchronized void cancel(Throwable e) {
            onCancel.accept(e);
            canceled = true;
        }
    }

    public LoadBalancer(Set<String> clusterUrls, int threadCount, int executingTaskPerNode) {
        this.nodeUrlToTasks = new HashMap<>();
        this.executorService = Executors.newFixedThreadPool(threadCount);

        for (String url : clusterUrls) {
            this.nodeUrlToTasks.put(url, new LinkedBlockingQueue<>(executingTaskPerNode));
        }
    }

    public void scheduleTask(String url, CancelableTask cancelableTask) {
        final BlockingQueue<CancelableTask> nodeTasks = nodeUrlToTasks.get(url);
        if (!nodeTasks.offer(cancelableTask)) {
            cancelableTask.cancel(new CancellationException("Canceled"));
            return;
        }

        try {
            executorService.execute(() -> {
                final CancelableTask currentTask = nodeTasks.poll();

                try {
                    currentTask.run();
                } catch (Exception e) {
                    currentTask.onCancel.accept(e);
                }
            });
        } catch (RejectedExecutionException e) {
            cancelableTask.onCancel.accept(e);
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
