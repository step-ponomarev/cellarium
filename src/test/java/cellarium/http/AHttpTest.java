package cellarium.http;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import cellarium.http.conf.ServerConfiguration;
import cellarium.http.conf.ServiceConfig;
import cellarium.http.service.HttpService;
import one.nio.http.HttpServer;

public abstract class AHttpTest {
    private static final int PORT = 8080;
    private static final String URL = "http://localhost:" + PORT;
    private static final String ENDPOINT = URL + ServerConfiguration.V_0_ENTITY_ENDPOINT;
    private static final int BODY_LEN_BYTES = 40;

    private static HttpServer server;

    protected final HttpService httpService = new HttpService(ENDPOINT);

    @BeforeClass
    public static void startServer() throws IOException {
        final ServiceConfig config = new ServiceConfig(
                PORT,
                URL,
                Collections.singletonList(URL),
                Files.createTempDirectory("TMP_DIR")
        );
        server = new Server(config);

        server.start();
    }

    @AfterClass
    public static void stopServer() {
        server.stop();
        server = null;
    }

    protected static byte[] generateBody() {
        return generateRandomBytes(BODY_LEN_BYTES);
    }

    protected static String generateId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    private static byte[] generateRandomBytes(int len) {
        if (len <= 0) {
            throw new IllegalArgumentException("Len should be more than 0");
        }

        byte[] bytes = new byte[len];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
