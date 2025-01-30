package cellarium.db.exception;

public final class TimeoutException extends RuntimeException {
    public TimeoutException(String message) {
        super(message);
    }
}
