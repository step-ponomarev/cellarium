package cellarium.http;

import org.junit.Test;

public class AsyncTest extends AHttpTest {
    //TODO: Доделать тест
    @Test(timeout = 60000)
    public void testBadRequestWhenServerShuttingDown() {
        this.stopServer();
    }
}
