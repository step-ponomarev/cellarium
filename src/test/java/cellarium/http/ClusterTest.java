package cellarium.http;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import cellarium.http.service.Cluster;
import cellarium.http.service.EndpointService;

public class ClusterTest extends AHttpTest {
    private static final String BASE_URL = "http://localhost:";
    private static final Set<Integer> PORTS = Set.of(8080, 8081, 8082, 8083, 8084);
    private static final List<String> CLUSTER_URLS = PORTS.stream().map(p -> BASE_URL + p).toList();

    private static final Path CLUSTER_TEST_DIR = Paths.get(
            "./src/test/resources").toAbsolutePath().normalize().resolve(
            Path.of("test_dir")
    );

    @Test
    public final void testPutAndGetFromEachShard() throws IOException, InterruptedException {
        try (final ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, Files.createDirectory(CLUSTER_TEST_DIR))) {
            cluster.start();
            final String id = generateId();
            final byte[] body = generateBody();

            EndpointService service = cluster.getRandomEndpoint();
            final HttpResponse<byte[]> putResponse = service.put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

            for (String url : CLUSTER_URLS) {
                final EndpointService exactEndpoint = cluster.getExactEndpoint(url);
                final HttpResponse<byte[]> getResponse = exactEndpoint.get(id);
                Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
                Assert.assertArrayEquals(body, getResponse.body());
            }
        }
    }

    @Test(expected = ConnectException.class)
    public final void testClusterStop() throws IOException, InterruptedException {
        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, Files.createDirectory(CLUSTER_TEST_DIR))) {
            cluster.start();

            final HttpResponse<byte[]> putResponse = cluster.getRandomEndpoint().put(generateId(), generateBody());
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());
            cluster.stop();

            cluster.getRandomEndpoint().put(generateId(), generateBody());
        }
    }

    @Test
    public final void testClusterKeepDataAfterRestart() throws IOException, InterruptedException {
        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, Files.createDirectory(CLUSTER_TEST_DIR))) {
            cluster.start();

            final String id = generateId();
            final byte[] body = generateBody();

            final HttpResponse<byte[]> putResponse = cluster.getRandomEndpoint().put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

            HttpResponse<byte[]> getResponse = cluster.getRandomEndpoint().get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
            Assert.assertArrayEquals(body, getResponse.body());

            cluster.stop();
            cluster.start();

            getResponse = cluster.getRandomEndpoint().get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
            Assert.assertArrayEquals(body, getResponse.body());
        }
    }

    @Test
    public final void testNotAllNodesKeepData() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(CLUSTER_TEST_DIR);

        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, workDir)) {
            cluster.start();

            final String id = generateId();
            final byte[] body = generateBody();

            final HttpResponse<byte[]> putResponse = cluster.getRandomEndpoint().put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

            cluster.stop();

            int replicasCount = 0;
            for (String url : CLUSTER_URLS) {
                final Cluster urlCluster = new Cluster(Collections.singletonList(url), workDir);
                urlCluster.start();

                HttpResponse<byte[]> httpResponse = urlCluster.getExactEndpoint(url).get(id);
                if (httpResponse.statusCode() == HttpURLConnection.HTTP_OK) {
                    replicasCount++;
                }

                urlCluster.stop();
            }

            Assert.assertTrue(replicasCount > 0);
            Assert.assertTrue(replicasCount < CLUSTER_URLS.size());
        }
    }

    private static class ClearableCluster extends Cluster implements Closeable {
        public ClearableCluster(List<String> clusterUrls, Path baseDir) throws IOException {
            super(clusterUrls, baseDir);
        }

        @Override
        public void close() throws IOException {
            if (this.running) {
                this.stop();
            }
            this.clearData();
        }
    }
}
