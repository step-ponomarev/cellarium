package http.conf;

import java.util.Set;
import one.nio.http.Request;

public final class ServerConfiguration {
    public static final String V_0_ENTITY_ENDPOINT = "/v0/entity";
    public static final Set<Integer> SUPPORTED_METHODS = Set.of(Request.METHOD_DELETE, Request.METHOD_GET, Request.METHOD_PUT);

    private ServerConfiguration() {}
}
