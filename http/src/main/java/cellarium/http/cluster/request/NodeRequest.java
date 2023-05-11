package cellarium.http.cluster.request;

import cellarium.http.QueryParam;
import one.nio.http.Request;

public class NodeRequest extends Request {
    private final String id;
    private final long timestamp;

    private NodeRequest(Request prototype, String id, long timestamp) {
        super(prototype);
        this.id = id;
        this.timestamp = timestamp;
    }

    public static NodeRequest of(Request prototype, String id, long timestamp) {
        final String url = String.format("%s?%s=%s&%s=%s", prototype.getPath(), QueryParam.ID, id, QueryParam.TIMESTAMP, timestamp);
        final Request request = new Request(prototype.getMethod(), url, prototype.isHttp11());

        final String[] headers = prototype.getHeaders();
        for (String header : headers) {
            if (header != null) {
                request.addHeader(header);
            }
        }
        request.setBody(request.getBody());

        return new NodeRequest(
                request,
                id,
                timestamp
        );
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
