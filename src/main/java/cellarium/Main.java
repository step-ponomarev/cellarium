package cellarium;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import cellarium.http.Server;
import cellarium.http.conf.ServerConfig;

public class Main {
    private static final int PORT = 8080;
    private static final String URL = "http://localhost:" + PORT;

    public static void main(String[] args) throws IOException {
        final ServerConfig config = new ServerConfig(
                PORT,
                URL,
                Collections.singletonList(URL),
                Files.createTempDirectory("TMP_DIR"),
                Runtime.getRuntime().availableProcessors() - 2
        );

        final Server server = new Server(config);
        server.start();
    }
}
