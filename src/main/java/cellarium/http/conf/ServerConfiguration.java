package cellarium.http.conf;

import java.util.Map;
import java.util.Set;
import one.nio.http.Request;

public class ServerConfiguration {
    public static final String V_0_ENTITY_ENDPOINT = "/v0/entity";
    public static final Map<String, Set<Integer>> SUPPORTED_METHODS_BY_ENDPOINT = Map.of(
            V_0_ENTITY_ENDPOINT,
            Set.of(Request.METHOD_DELETE, Request.METHOD_GET, Request.METHOD_PUT)
    );

    public static final long DAO_INMEMORY_LIMIT_BYTES = 1 * 1024 * 1024; // 4MB

    private ServerConfiguration() {}
}
