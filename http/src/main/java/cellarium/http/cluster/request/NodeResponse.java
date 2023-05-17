package cellarium.http.cluster.request;

import one.nio.http.Response;

public final class NodeResponse extends Response {
    public static final String TIMESTAMP_HEADER = "X-Timestamp";
    private final long timestamp;

    NodeResponse(String resultCode, byte[] body, long timestamp) {
        super(resultCode, body);
        this.timestamp = timestamp;
    }

    NodeResponse(Response response, long timestamp) {
        super(response);
        this.timestamp = timestamp;
    }

    public static NodeResponse createLocalResponse(String resultCode, byte[] body, long timestamp) {
        final NodeResponse nodeResponse = new NodeResponse(resultCode, body, timestamp);
        nodeResponse.addHeader(TIMESTAMP_HEADER + ": " + timestamp);

        return nodeResponse;
    }

    public static NodeResponse wrapRemoteResponse(Response response) throws NumberFormatException {
        final String timestamp = getHeader(response, TIMESTAMP_HEADER);

        return new NodeResponse(response, Long.parseLong(timestamp));
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getHeader(String key) {
        return NodeResponse.getHeader(this, key);
    }

    private static String getHeader(Response response, String key) {
        final int keyLength = key.length();

        final int headerCount = response.getHeaderCount();
        final String[] headers = response.getHeaders();
        for (int i = 1; i < headerCount; i++) {
            if (headers[i].regionMatches(true, 0, key, 0, keyLength)) {
                return headers[i].split(":")[1].trim();
            }
        }
        return null;
    }
}
