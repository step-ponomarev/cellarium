package cellarium.http.cluster.request;

import one.nio.http.Request;

public class NodeRequest extends Request {
    private final String id;
    private final long timestamp;

    NodeRequest(Request prototype, String id, long timestamp) {
        super(prototype);
        this.id = id;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
