package cellarium.dao;

public final class LockedTask implements Runnable {
    private final Object lock;
    private final Runnable task;

    public LockedTask(Object lock, Runnable task) {
        this.lock = lock;
        this.task = task;
    }

    @Override
    public void run() {
        synchronized (lock) {
            task.run();
        }
    }
}
