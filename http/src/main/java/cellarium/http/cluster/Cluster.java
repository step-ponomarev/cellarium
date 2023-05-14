package cellarium.http.cluster;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Cluster {
    private final Node[] nodes;
    private final Map<String, Set<Node>> urlToReplicas;

    private final Map<String, Node> urlToNode;
    private final Map<String, Set<String>> urlToReplicaUrls;

    public Cluster(Node[] nodes, Map<String, Set<Node>> urlToReplicas) {
        this.nodes = nodes;
        this.urlToReplicas = urlToReplicas;
        //TODO: Пофиксить, убирать дубли
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

    public Set<String> getReplicaUrlsByUrl(String url) {
        final Set<String> replicaUrls = urlToReplicaUrls.get(url);

        return replicaUrls == null ? Collections.emptySet() : replicaUrls;
    }

    public Set<Node> getReplicasByUrl(String url) {
        final Set<Node> nodes = urlToReplicas.get(url);
        if (nodes == null) {
            return Collections.emptySet();
        }

        return nodes;
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
