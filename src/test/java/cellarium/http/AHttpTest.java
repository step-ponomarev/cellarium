package cellarium.http;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.AfterClass;
import cellarium.dao.DiskUtils;

public abstract class AHttpTest {
    private static final int BODY_LEN_BYTES = 40;

    public static final Path DEFAULT_DIR = Path.of("tmp");

    @AfterClass
    public static void cleanUp() throws IOException {
        if (Files.exists(DEFAULT_DIR)) {
            DiskUtils.removeDir(DEFAULT_DIR);
        }
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
