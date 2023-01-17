package cellarium.http;

import java.net.http.HttpClient;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AHttpTest {
    protected final HttpClient httpClient = HttpClient.newHttpClient();

    protected static byte[] generateBody(int len) {
        if (len <= 0) {
            throw new IllegalArgumentException("Len should be more than 0");
        }

        byte[] bytes = new byte[len];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    protected static String generateId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }
}
