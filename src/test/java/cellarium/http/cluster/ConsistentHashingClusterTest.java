package cellarium.http.cluster;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class ConsistentHashingClusterTest {
    @Test
    public void testVirtualNodeCreation() {
        final int virtualNodePerRealNodeAmount = 3;
        final Set<String> clusterUrls = IntStream.range(0, 9000).mapToObj(i -> "http://localhost:" + i).collect(Collectors.toSet());

        final Node[] sortedVirtualNodes = ConsistentHashingCluster.createSortedVirtualNodes("http://localhost:0", clusterUrls, virtualNodePerRealNodeAmount);
        Assert.assertEquals(clusterUrls.size() * virtualNodePerRealNodeAmount, sortedVirtualNodes.length);

        for (int i = 1; i < sortedVirtualNodes.length; i++) {
            Assert.assertTrue(sortedVirtualNodes[i - 1].hashCode() < sortedVirtualNodes[i].hashCode());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullClusterUrlsArgumentCreation() {
        ConsistentHashingCluster.createSortedVirtualNodes("http://localhost:0", null, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyClusterUrlsArgumentCreation() {
        ConsistentHashingCluster.createSortedVirtualNodes("http://localhost:0", Collections.emptySet(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroVirtualNodesCountArgumentCreation() {
        ConsistentHashingCluster.createSortedVirtualNodes("http://localhost:0", Set.of("http://localhost:0"), 0);
    }

    @Test
    public void testVirtualNodesCoverAllHashes() {
        final int virtualNodePerRealNodeAmount = 3;
        final Set<String> clusterUrls = IntStream.range(0, 9000).mapToObj(i -> "http://localhost:" + i).collect(Collectors.toSet());

        final Node[] virtualNodes = ConsistentHashingCluster.createSortedVirtualNodes("http://localhost:0", clusterUrls, virtualNodePerRealNodeAmount);
        final int firstNodeHash = virtualNodes[0].hashCode();

        final int step = 65536;
        for (int hash = Integer.MIN_VALUE; true; hash += step) {
            final int nodeIndexForHash = ConsistentHashingCluster.getNodeIndexForHash(virtualNodes, hash);

            Node virtualNode = virtualNodes[nodeIndexForHash];
            final int currentNodeHashCode = virtualNode.hashCode();

            Assert.assertTrue(currentNodeHashCode >= hash || currentNodeHashCode == firstNodeHash);
            if (nodeIndexForHash > 0) {
                virtualNode = virtualNodes[nodeIndexForHash - 1];
                Assert.assertTrue(virtualNode.hashCode() < hash);
            }

            // Еще итерация и будет переполнение
            if (Integer.MAX_VALUE - step < hash) {
                break;
            }
        }

        final int nodeIndexForHash = ConsistentHashingCluster.getNodeIndexForHash(virtualNodes, Integer.MAX_VALUE);
        int currentNodeHashCode = virtualNodes[nodeIndexForHash].hashCode();
        Assert.assertTrue(currentNodeHashCode == Integer.MAX_VALUE || firstNodeHash == currentNodeHashCode);
    }
}
