package cellarium;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import cellarium.http.Server;
import cellarium.http.conf.ServiceConfig;

public class Main {
    private static final int PORT = 8080;
    private static final String URL = "http://localhost:" + PORT;

    public static void main(String[] args) throws IOException {
        final ServiceConfig config = new ServiceConfig(
                PORT,
                URL,
                Collections.singletonList(URL),
                Files.createTempDirectory("TMP_DIR"),
                Runtime.getRuntime().availableProcessors()
        );

        final Server server = new Server(config);
        server.start();
    }
}
