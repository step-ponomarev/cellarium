package cellarium;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import cellarium.conf.AnnotatedDaoConfig;
import cellarium.conf.ConfigReader;
import cellarium.dao.MemorySegmentDao;
import cellarium.http.Server;
import sun.misc.Signal;

public class Main {
    public static final ConfigReader configReader = new ConfigReader();

    public static void main(String[] args) throws IOException {
        final AnnotatedDaoConfig daoConfig = configReader.readDaoConfig();

        final Path path = Path.of(daoConfig.path);
        if (Files.notExists(path)) {
            Files.createDirectory(path);
        }

        final Server server = new Server(
                configReader.readServerConfig(),
                new MemorySegmentDao(configReader.readDaoConfig())
        );
        server.start();

        // handle kill <PID>
        Signal.handle(new Signal("TERM"), (Signal signal) -> {
            System.out.println("here");
            server.stop();
        });
    }
}
