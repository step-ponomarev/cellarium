package cellarium.http;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import cellarium.db.utils.DiskUtils;
import cellarium.http.service.Cluster;
import cellarium.http.service.EndpointService;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClusterTest extends AHttpTest {
    private static final String BASE_URL = "http://127.0.0.1";
    private static final Set<Integer> PORTS = Set.of(8080, 8081, 8082, 8083, 8084);
    private static final Set<String> CLUSTER_URLS = PORTS.stream()
            .map(p -> BASE_URL + ":" + p)
            .collect(Collectors.toSet());

    @Test
    public final void testPutAndGetFromEachShard() throws IOException, InterruptedException {
        try (final ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, Files.createDirectory(DEFAULT_DIR))) {
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
        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, Files.createDirectory(DEFAULT_DIR))) {
            cluster.start();

            final HttpResponse<byte[]> putResponse = cluster.getRandomEndpoint().put(generateId(), generateBody());
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());
            cluster.stop();

            cluster.getRandomEndpoint().put(generateId(), generateBody());
        }
    }

    @Test()
    public final void testClusterKeepDataAfterRestart() throws IOException, InterruptedException {
        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, Files.createDirectory(DEFAULT_DIR))) {
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

    @Test()
    public final void testNotAllNodesKeepData() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(DEFAULT_DIR);

        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, workDir)) {
            cluster.start();

            final String id = generateId();
            final byte[] body = generateBody();

            final HttpResponse<byte[]> putResponse = cluster.getRandomEndpoint().put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

            cluster.stop();

            int replicasCount = 0;
            for (String url : CLUSTER_URLS) {
                final Cluster urlCluster = new Cluster(Map.of(url, Collections.emptySet()), workDir);
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

    @Test
    public final void testEachNodeHasData() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(DEFAULT_DIR);

        try (ClearableCluster cluster = new ClearableCluster(CLUSTER_URLS, workDir)) {
            cluster.start();

            final int count = 20_000;
            final List<String> ids = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final String id = generateId();
                final byte[] body = generateBody();

                final HttpResponse<byte[]> putResponse = cluster.getRandomEndpoint().put(id, body);
                Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

                ids.add(id);
            }

            cluster.stop();

            for (String url : CLUSTER_URLS) {
                final Cluster singleInstanceCluster = new Cluster(Map.of(url, Collections.emptySet()), workDir);
                singleInstanceCluster.start();

                final EndpointService endpoint = singleInstanceCluster.getExactEndpoint(url);

                boolean isEmpty = true;
                for (String id : ids) {
                    HttpResponse<byte[]> response = endpoint.get(id);

                    if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                        isEmpty = false;
                        break;
                    }
                }

                singleInstanceCluster.stop();

                Assert.assertFalse(isEmpty);
            }
        }
    }

    @Test
    public final void testSimpleReplication() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(DEFAULT_DIR);

        final String firstNode = "http://127.0.0.1:8080";
        final String secondNode = "http://127.0.0.1:8081";
        final Map<String, Set<String>> stringSetMap = Map.of(
                firstNode, Set.of(secondNode),
                secondNode, Set.of(firstNode)
        );

        try {
            Cluster cluster = new Cluster(stringSetMap, workDir, 2);
            cluster.start();

            final int count = 20_000;
            final List<String> ids = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final String id = generateId();
                final byte[] body = generateBody();

                final HttpResponse<byte[]> putResponse = cluster.getExactEndpoint(firstNode).put(id, body);
                Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

                ids.add(id);
            }

            cluster.stop();
            
            // check only first node
            cluster = new Cluster(Map.of(firstNode, Collections.emptySet()), workDir);
            cluster.start();

            for (String id : ids) {
                final HttpResponse<byte[]> putResponse = cluster.getExactEndpoint(firstNode).get(id);
                Assert.assertEquals(HttpURLConnection.HTTP_OK, putResponse.statusCode());
            }
            cluster.stop();

            // check only second node
            cluster = new Cluster(Map.of(secondNode, Collections.emptySet()), workDir);
            cluster.start();

            for (String id : ids) {
                final HttpResponse<byte[]> putResponse = cluster.getExactEndpoint(secondNode).get(id);
                Assert.assertEquals(HttpURLConnection.HTTP_OK, putResponse.statusCode());
            }
            cluster.stop();
        } finally {
            DiskUtils.removeDir(workDir);
        }
    }
    
    @Test
    public final void testNewestReplacedData() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(DEFAULT_DIR);

        final String firstNode = "http://127.0.0.1:8080";
        final String secondNode = "http://127.0.0.1:8081";
        final Map<String, Set<String>> stringSetMap = Map.of(
                firstNode, Set.of(secondNode),
                secondNode, Set.of(firstNode)
        );

        try {
            Cluster cluster = new Cluster(stringSetMap, workDir, 2);
            cluster.start();

            final String id = generateId();
            final byte[] body = generateBody();

            HttpResponse<byte[]> putResponse = cluster.getExactEndpoint(firstNode).put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());
            cluster.stop();

            cluster = new Cluster(Map.of(secondNode, Collections.emptySet()), workDir);
            cluster.start();

            final EndpointService secondNodeEndpoint = cluster.getExactEndpoint(secondNode);
            HttpResponse<byte[]> getResponse = secondNodeEndpoint.get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
            Assert.assertArrayEquals(body, getResponse.body());

            final byte[] newBody = generateBody();
            putResponse = secondNodeEndpoint.put(id, newBody);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());
            
            getResponse = secondNodeEndpoint.get(id);
            Assert.assertArrayEquals(newBody, getResponse.body());
            cluster.stop();
            
            cluster = new Cluster(stringSetMap, workDir, 2);
            cluster.start();
            
            getResponse = cluster.getExactEndpoint(firstNode).get(id);
            Assert.assertArrayEquals(newBody, getResponse.body());
            
            getResponse = cluster.getExactEndpoint(secondNode).get(id);
            Assert.assertArrayEquals(newBody, getResponse.body());
            cluster.stop();
        } finally {
            DiskUtils.removeDir(workDir);
        }
    }
    
    @Test
    public final void testNewestDeletedData() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(DEFAULT_DIR);

        final String firstNode = "http://127.0.0.1:8080";
        final String secondNode = "http://127.0.0.1:8081";
        final Map<String, Set<String>> stringSetMap = Map.of(
                firstNode, Set.of(secondNode),
                secondNode, Set.of(firstNode)
        );

        try {
            Cluster cluster = new Cluster(stringSetMap, workDir, 2);
            cluster.start();

            final String id = generateId();
            final byte[] body = generateBody();

            HttpResponse<byte[]> modificationResponse = cluster.getExactEndpoint(firstNode).put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, modificationResponse.statusCode());
            cluster.stop();

            cluster = new Cluster(Map.of(secondNode, Collections.emptySet()), workDir);
            cluster.start();

            final EndpointService secondNodeEndpoint = cluster.getExactEndpoint(secondNode);
            HttpResponse<byte[]> getResponse = secondNodeEndpoint.get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
            Assert.assertArrayEquals(body, getResponse.body());
            
            modificationResponse = secondNodeEndpoint.delete(id);
            Assert.assertEquals(HttpURLConnection.HTTP_ACCEPTED, modificationResponse.statusCode());
            
            getResponse = secondNodeEndpoint.get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());
            cluster.stop();

            cluster = new Cluster(stringSetMap, workDir, 2);
            cluster.start();

            getResponse = cluster.getExactEndpoint(firstNode).get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());

            getResponse = cluster.getExactEndpoint(secondNode).get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());
            cluster.stop();
        } finally {
            DiskUtils.removeDir(workDir);
        }
    }
    
    @Test
    public final void testAddedNewReplicaWithoutData() throws IOException, InterruptedException {
        final Path workDir = Files.createDirectory(DEFAULT_DIR);

        final String firstNode = "http://127.0.0.1:8080";
        final String secondNode = "http://127.0.0.1:8081";
        final Map<String, Set<String>> stringSetMap = Map.of(
                firstNode, Set.of(secondNode),
                secondNode, Set.of(firstNode)
        );

        try {
            Cluster cluster = new Cluster(Map.of(firstNode, Collections.emptySet()), workDir);
            cluster.start();

            final String id = generateId();
            final byte[] body = generateBody();

            HttpResponse<byte[]> modificationResponse = cluster.getExactEndpoint(firstNode).put(id, body);
            Assert.assertEquals(HttpURLConnection.HTTP_CREATED, modificationResponse.statusCode());
            cluster.stop();

            cluster = new Cluster(stringSetMap, workDir, 2);
            cluster.start();
            
            HttpResponse<byte[]> getResponse = cluster.getExactEndpoint(secondNode).get(id);
            Assert.assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
            Assert.assertArrayEquals(body, getResponse.body());
            
            cluster.stop();
        } finally {
            DiskUtils.removeDir(workDir);
        }
    }

    private static class ClearableCluster extends Cluster implements Closeable {
        public ClearableCluster(Set<String> clusterUrls, Path baseDir) {
            super(clusterUrls.stream().collect(Collectors.toMap(UnaryOperator.identity(), e -> Collections.emptySet())), baseDir);
        }

        @Override
        public void close() throws IOException {
            if (this.running) {
                this.stop();
            }

            DiskUtils.removeDir(baseDir);
        }
    }
}
