package cellarium.http;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AHttpTest {
    private static final int BODY_LEN_BYTES = 40;

    protected static final Path TEST_DIR = Paths.get(
            "./src/test/resources").toAbsolutePath().normalize().resolve(
            Path.of("test_dir")
    );

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
