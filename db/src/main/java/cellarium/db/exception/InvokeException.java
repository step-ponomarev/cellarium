package cellarium.db.exception;

public final class InvokeException extends RuntimeException {
    public InvokeException(String msg, Throwable cause) {
        super(cause);
    }
}
