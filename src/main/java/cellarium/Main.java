package cellarium;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import cellarium.dao.DaoConfig;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.Server;
import cellarium.http.conf.ServerConfig;
import one.nio.server.AcceptorConfig;

public class Main {
    private static final int PORT = 8080;
    private static final String URL = "http://localhost:" + PORT;

    public static void main(String[] args) throws IOException {
        final DaoConfig daoConfig = createDaoConfig();
        if (Files.notExists(daoConfig.path)) {
            Files.createDirectory(daoConfig.path);
        }

        final MemorySegmentDao memorySegmentDao = new MemorySegmentDao(daoConfig);
        final ServerConfig config = createServerConfig();
        final Server server = new Server(config, memorySegmentDao);
        server.start();
    }

    private static DaoConfig createDaoConfig() throws IOException {
        final DaoConfig daoConfig = new DaoConfig();
        daoConfig.path = Files.createTempDirectory("TMP_DIR");
        daoConfig.memtableLimitBytes = 1024 * 1024;

        return daoConfig;
    }

    private static ServerConfig createServerConfig() {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.selfPort = PORT;
        serverConfig.selfUrl = URL;
        serverConfig.clusterUrls = Collections.singleton(URL);
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
