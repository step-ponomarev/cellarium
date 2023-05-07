package cellarium.http.cluster;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class ConsistentHashingClusterTest {
    @Test(expected = IllegalArgumentException.class)
    public void testNullClusterUrlsArgumentCreation() {
        ConsistentHashingCluster.createVirtualNodes("http://localhost:0", null, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyClusterUrlsArgumentCreation() {
        ConsistentHashingCluster.createVirtualNodes("http://localhost:0", Collections.emptySet(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroVirtualNodesCountArgumentCreation() {
        ConsistentHashingCluster.createVirtualNodes("http://localhost:0", Set.of("http://localhost:0"), 0);
    }

    @Test
    public void testVirtualNodesCoverAllHashes() {
        final int virtualNodePerRealNodeAmount = 3;
        final Set<String> clusterUrls = IntStream.range(0, 9000).mapToObj(i -> "http://localhost:" + i).collect(Collectors.toSet());
        final Node[] virtualNodes = ConsistentHashingCluster.createVirtualNodes("http://localhost:0", clusterUrls, virtualNodePerRealNodeAmount);

        final int step = 100;
        for (int hash = Integer.MIN_VALUE; true; hash += step) {
            final int nodeIndexForHash = ConsistentHashingCluster.getNodeIndexForHash(hash, virtualNodes.length);
            Assert.assertTrue(nodeIndexForHash >= 0);
            Assert.assertTrue(nodeIndexForHash < virtualNodes.length);

            // Еще итерация и будет переполнение
            if (Integer.MAX_VALUE - step < hash) {
                break;
            }
        }

        final int nodeIndexForHash = ConsistentHashingCluster.getNodeIndexForHash(Integer.MAX_VALUE, virtualNodes.length);
        Assert.assertTrue(nodeIndexForHash < virtualNodes.length);
        Assert.assertTrue(nodeIndexForHash >= 0);
    }

    @Test
    public void testCheckNodeUsing() {
        final int virtualNodePerRealNodeAmount = 3;
        final Set<String> clusterUrls = IntStream.range(0, 3).mapToObj(i -> "http://localhost:" + i).collect(Collectors.toSet());
        final Node[] virtualNodes = ConsistentHashingCluster.createVirtualNodes("http://localhost:0", clusterUrls, virtualNodePerRealNodeAmount);

        final int[] nodeIndexes = new int[virtualNodes.length];

        final int step = 100;
        for (int hash = Integer.MIN_VALUE; true; hash += step) {
            final int nodeIndexForHash = ConsistentHashingCluster.getNodeIndexForHash(hash, virtualNodes.length);
            nodeIndexes[nodeIndexForHash]++;

            // Еще итерация и будет переполнение
            if (Integer.MAX_VALUE - step < hash) {
                break;
            }
        }

        final int nodeIndexForHash = ConsistentHashingCluster.getNodeIndexForHash(Integer.MAX_VALUE, virtualNodes.length);
        nodeIndexes[nodeIndexForHash]++;

        for (int i = 0; i < nodeIndexes.length; i++) {
            System.out.println(i + " : " + nodeIndexes[i]);
        }
    }
}
