package db;

public final class LockedTask implements Runnable {
    private final Object lock;
    private final Runnable task;

    public LockedTask(Runnable task, Object lock) {
        this.task = task;
        this.lock = lock;
    }

    @Override
    public void run() {
        synchronized (lock) {
            task.run();
        }
    }
}
