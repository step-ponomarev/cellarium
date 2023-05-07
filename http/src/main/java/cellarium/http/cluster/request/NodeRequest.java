package cellarium.http.cluster.request;

import one.nio.http.Request;

public class NodeRequest extends Request {
    private final String id;

    public NodeRequest(Request prototype, String id) {
        super(prototype);
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
