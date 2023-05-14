package cellarium.http.cluster;

import one.nio.http.Response;

public final class NodeResponse extends Response {
    public static final String TIMESTAMP_HEADER = "X-Timestamp";
    private final long timestamp;

    public NodeResponse(Response prototype, long timestamp) {
        super(prototype);
        this.addHeader(TIMESTAMP_HEADER + ": " + timestamp);
        this.timestamp = timestamp;
    }

    public NodeResponse(String resultCode, byte[] body, long timestamp) {
        this(new Response(resultCode, body), timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
