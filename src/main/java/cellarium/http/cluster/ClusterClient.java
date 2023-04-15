package cellarium.http.cluster;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import one.nio.http.HttpClient;
import one.nio.util.Hash;

//TODO в сегмент писать виртуальные ноды
// сегменты засунуть в массив и упорядочить по хешу
// искать бинарным поиском нужный сегмент, проверяя соседей ( так мы ускорим нахождение нужного сегмента )
// Дальше берем нужные виртуальные ноды и пишем/читаем из них.
public final class ClusterClient {
    private final String[] clusterUrls;
    private final Map<String, VirtualNode> nodes;

    public ClusterClient(String selfUrl, Set<String> clusterUrls, int virtualNodesCount) {
        this.clusterUrls = clusterUrls.toArray(String[]::new);
        this.nodes = clusterUrls.stream().filter(u -> !Objects.equals(selfUrl, u))
                .collect(Collectors.toMap(UnaryOperator.identity(), u -> new VirtualNode(u, virtualNodesCount)));
    }

    //TODO: нужный сегмент нужно искать бинарным поиском.
    public String getClusterUrl(String id) {
        if (clusterUrls.length == 1) {
            return clusterUrls[0];
        }

        return clusterUrls[(Hash.murmur3(id) & Integer.MAX_VALUE) % clusterUrls.length];
    }

    public HttpClient getNode(String url) {
        return nodes.get(url);
    }

    private static final class Segment {
        private 

        public Segment(int segmentStart) {
            this.segmentStart = segmentStart;
        }

        public int getSegmentStart() {
            return segmentStart;
        }
    }
}
