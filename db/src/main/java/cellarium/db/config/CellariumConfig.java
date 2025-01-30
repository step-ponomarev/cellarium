package cellarium.db.config;

import java.nio.file.Path;

public final class CellariumConfig {
    public final long flushSizeBytes;
    public final Path databasePath;

    public CellariumConfig(long flushSizeBytes, Path databasePath) {
        this.flushSizeBytes = flushSizeBytes;
        this.databasePath = databasePath;
    }
}
