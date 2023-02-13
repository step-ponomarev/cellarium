package cellarium.http;

import java.io.IOException;
import org.junit.Test;

public class AsyncTest extends AHttpTest {
    //TODO: Доделать тест
    @Test(timeout = 60000)
    public void testBadRequestWhenServerShuttingDown() throws IOException, InterruptedException {
        this.stopServer();
    }
}
