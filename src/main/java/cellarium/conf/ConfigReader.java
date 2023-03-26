package cellarium.conf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import cellarium.dao.DaoConfig;
import cellarium.http.conf.ServerConfig;
import one.nio.config.ConfigParser;

public final class ConfigReader {
    private static final String SERVER_CONF = "server.config.yaml";
    private static final String DAO_CONF = "dao.config.yaml";

    private final Path confDir;

    public ConfigReader(Path confDir) {
        if (confDir == null || Files.notExists(confDir)) {
            throw new IllegalArgumentException("Config directory does not exist");
        }

        this.confDir = confDir;
    }

    public AnnotatedDaoConfig readDaoConfig() throws IOException {
        final Path conf = confDir.resolve(DAO_CONF);
        if (Files.notExists(conf)) {
            throw new IllegalStateException("Dao config does not exist: " + conf);
        }

        return ConfigParser.parse(Files.readString(conf), AnnotatedDaoConfig.class);
    }

    public ServerConfig readServerConfig() throws IOException {
        final Path conf = confDir.resolve(SERVER_CONF);
        if (Files.notExists(conf)) {
            throw new IllegalStateException("Server config does not exist: " + conf);
        }

        return ConfigParser.parse(Files.readString(conf), ServerConfig.class);
    }
}
