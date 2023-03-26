package cellarium.http.handlers;

public final class RequestHandlingException extends Exception {
    public RequestHandlingException(Throwable cause) {
        super(cause);
    }
}