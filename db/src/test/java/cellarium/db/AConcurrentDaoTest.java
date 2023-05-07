package cellarium.db;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public abstract class AConcurrentDaoTest extends ADaoTest {
    public Await runAsync(int threadCount, int tasksCount, AsyncTask runnable) {
        final ExecutorService service = Executors.newFixedThreadPool(threadCount);
        final List<Callable<Object>> tasks = IntStream.range(0, tasksCount).mapToObj(i -> (Callable<Object>) () -> {
            try {
                runnable.run(i);
                return null;
            } catch (Exception e) {
                return e;
            }
        }).toList();

        try {
            final List<Future<Object>> results = service.invokeAll(tasks);
            service.shutdown();

            return () -> {
                for (Future<Object> result : results) {
                    Object o = result.get();

                    if (o instanceof Exception) {
                        throw (Exception) o;
                    }
                }
            };
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    protected interface AsyncTask {
        void run(int index) throws Exception;
    }

    protected interface Await {
        void await() throws Exception;
    }
}
