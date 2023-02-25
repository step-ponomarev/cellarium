package cellarium.http;

import java.util.concurrent.ThreadLocalRandom;

public abstract class AHttpTest {
    private static final int BODY_LEN_BYTES = 40;

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
