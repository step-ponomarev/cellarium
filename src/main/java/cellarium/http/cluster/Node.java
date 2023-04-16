package cellarium.http.cluster;

public class Node {
    protected final String nodeUrl;
    protected final boolean localNode;

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
