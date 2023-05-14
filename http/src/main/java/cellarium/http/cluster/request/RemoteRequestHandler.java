package cellarium.http.cluster.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cellarium.http.cluster.NodeResponse;
import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;

public final class RemoteRequestHandler implements NodeRequestHandler {
    private static final String ERROR_MSG = "Remote request is failed";
    private static final Logger log = LoggerFactory.getLogger(RemoteRequestHandler.class);

    private final HttpClient httpClient;

    public RemoteRequestHandler(String nodeUrl) {
        this.httpClient = new HttpClient(new ConnectionString(nodeUrl));
    }

    @Override
    public NodeResponse handleReqeust(NodeRequest request) throws RequestInvokeException {
        try {
            final Response response = this.httpClient.invoke(request);

            final String lastModified = getHeader(response, NodeResponse.TIMESTAMP_HEADER);
            if (lastModified == null) {
                return new NodeResponse(response, System.currentTimeMillis());
            }

            return new NodeResponse(response, Long.parseLong(lastModified));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            log.error(ERROR_MSG, e);
            throw new RequestInvokeException(ERROR_MSG, e);
        } catch (Exception e) {
            log.error(ERROR_MSG, e);
            throw new RequestInvokeException(ERROR_MSG, e);
        }
    }

    private String getHeader(Response response, String key) {
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
