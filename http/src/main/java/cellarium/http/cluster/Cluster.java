package cellarium.http.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import cellarium.http.cluster.request.LocalNodeRequestHandler;
import cellarium.http.cluster.request.RemoteNodeRequestHandler;

public final class Cluster {
    private final String selfUrl;
    private final Node[] nodes;
    private final Map<String, Set<Node>> urlToReplicas;

    private final Map<String, Node> urlToNode;
    private final Map<String, Set<String>> urlToReplicaUrls;

    private Cluster(String selfUrl, Node[] nodes, Map<String, Set<Node>> urlToReplicas) {
        this.selfUrl = selfUrl;
        this.nodes = nodes;
        this.urlToReplicas = urlToReplicas;
        this.urlToNode = Stream.of(nodes)
                .collect(Collectors.toMap(Node::getNodeUrl, UnaryOperator.identity(), (l, r) -> l));
        this.urlToReplicaUrls = Stream.of(nodes).collect(
                Collectors.toMap(
                        Node::getNodeUrl,
                        u -> urlToReplicas.get(u.getNodeUrl()).stream().map(Node::getNodeUrl).collect(Collectors.toSet()),
                        (l, r) -> l
                )
        );
    }
    
    public static Cluster createCluster(String selfUrl,
                                         Map<String, Set<String>> urlToReplicas,
                                         int virtualNodeAmount,
                                         LocalNodeRequestHandler localNodeRequestHandler) {
        final Map<String, Node> urlToNode = urlToReplicas.keySet().stream()
                .collect(Collectors.toMap(
                        UnaryOperator.identity(),
                        url -> new Node(url, url.equals(selfUrl) ? localNodeRequestHandler : new RemoteNodeRequestHandler(url))
                ));

        final String[] nodeUrls = urlToReplicas.keySet().toArray(String[]::new);
        final Node[] virtualNodes = new Node[nodeUrls.length * virtualNodeAmount];
        // node order for 3 nodes and 2 virual for each: [1, 2, 3, 1, 2, 3]
        for (int i = 0; i < virtualNodes.length; i++) {
            virtualNodes[i] = urlToNode.get(
                    nodeUrls[i % nodeUrls.length]
            );
        }

        final Map<String, Set<Node>> urlToReplicaNodes = new HashMap<>();
        for (Map.Entry<String, Node> entry : urlToNode.entrySet()) {
            final String url = entry.getKey();
            final Set<String> replicaUrls = urlToReplicas.get(url);
            final Node[] replicas = new Node[replicaUrls.size() + 1];

            int i = 0;
            for (String replicaUrl : replicaUrls) {
                replicas[i++] = urlToNode.get(replicaUrl);
            }
            replicas[i] = entry.getValue();

            urlToReplicaNodes.put(url, Set.of(replicas));
        }

        return new Cluster(selfUrl, virtualNodes, urlToReplicaNodes);
    }

    public Set<String> getReplicaUrlsByUrl(String url) {
        final Set<String> replicaUrls = urlToReplicaUrls.get(url);

        return replicaUrls == null ? Collections.emptySet() : replicaUrls;
    }

    public String getSelfUrl() {
        return selfUrl;
    }

    public Node getNodeByUrl(String url) {
        return urlToNode.get(url);
    }

    public Node getNodeByIndex(int i) {
        return nodes[i];
    }

    public int getPhysicNodeAmount() {
        return urlToReplicas.size();
    }

    public int getVirtualNodeAmount() {
        return nodes.length;
    }
}
