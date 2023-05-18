package cellarium.http.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public final class LoadBalancer implements Closeable {
    private final Map<String, BlockingQueue<Runnable>> nodeUrlToTasks;
    private final ExecutorService executorService;

    public LoadBalancer(Set<String> clusterUrls, int threadCount, int executingTaskPerNode) {
        this.nodeUrlToTasks = new HashMap<>();
        this.executorService = Executors.newFixedThreadPool(threadCount);

        for (String url : clusterUrls) {
            this.nodeUrlToTasks.put(url, new LinkedBlockingQueue<>(executingTaskPerNode));
        }
    }

    public String getLeastLoadReplicaUrl(Set<String> replicas) {
        int maxRemainingCapacity = Integer.MIN_VALUE;
        String leastLoaded = null;
        for (String url : replicas) {
            final int remainingCapacity = nodeUrlToTasks.get(url).remainingCapacity();
            if (remainingCapacity > 0 && maxRemainingCapacity > maxRemainingCapacity) {
                leastLoaded = url;
                maxRemainingCapacity = remainingCapacity;
            }
        }

        return leastLoaded;
    }

    public String[] sortByLoadFactor(Set<String> replicas) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("Empty urls");
        }

        final List<Map.Entry<String, BlockingQueue<Runnable>>> filteredEntries = new ArrayList<>(replicas.size());
        for (Map.Entry<String, BlockingQueue<Runnable>> entry : nodeUrlToTasks.entrySet()) {
            if (replicas.contains(entry.getKey()) && entry.getValue().remainingCapacity() != 0) {
                filteredEntries.add(entry);
            }
        }

        filteredEntries.sort(Comparator.comparingInt(queue -> queue.getValue().remainingCapacity()));

        final int resultSize = filteredEntries.size();
        final String[] result = new String[resultSize];
        for (int i = 0; i < resultSize; i++) {
            result[i] = filteredEntries.get(i).getKey();
        }

        return result;
    }

    public boolean scheduleTask(String url, Runnable task) throws RejectedExecutionException {
        if (url == null) {
            throw new NullPointerException("Url cannot be null");
        }

        final BlockingQueue<Runnable> nodeTasks = nodeUrlToTasks.get(url);
        if (!nodeTasks.offer(task)) {
            return false;
        }

        executorService.execute(() -> nodeTasks.poll().run());
        return true;
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
