package cellarium.http.cluster;

public final class Node {
    private final String nodeUrl;
    private final boolean localNode;

    public Node(String nodeUrl, boolean localNode) {
        this.nodeUrl = nodeUrl;
        this.localNode = localNode;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public boolean isLocalNode() {
        return localNode;
    }
}
