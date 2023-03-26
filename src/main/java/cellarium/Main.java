package cellarium;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import cellarium.http.Server;
import cellarium.http.conf.ServerConfig;
import one.nio.server.AcceptorConfig;

public class Main {
    private static final int PORT = 8080;
    private static final String URL = "http://localhost:" + PORT;

    public static void main(String[] args) throws IOException {
        final ServerConfig config = createServerConfig();

        final Server server = new Server(config);
        server.start();
    }

    public static ServerConfig createServerConfig() throws IOException {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.selfPort = PORT;
        serverConfig.selfUrl = URL;
        serverConfig.clusterUrls = Collections.singleton(URL);
        serverConfig.workingDir = Files.createTempDirectory("TMP_DIR");

        // 1 MB
        serverConfig.memTableSizeBytes = 1024 * 1024;
        serverConfig.threadCount = Runtime.getRuntime().availableProcessors() - 2;

        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = PORT;
        acceptor.reusePort = true;

        serverConfig.acceptors = new AcceptorConfig[]{
                acceptor
        };

        serverConfig.closeSessions = true;

        return serverConfig;
    }
}
