package cellarium.http.cluster;

import java.util.Objects;

public final class VirtualNode {
    private final String nodeUrl;
    private final boolean localNode;
    private final int hash;

    public VirtualNode(String nodeUrl, boolean localNode, int hash) {
        this.nodeUrl = nodeUrl;
        this.localNode = localNode;
        this.hash = hash;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public boolean isLocalNode() {
        return localNode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualNode)) return false;
        VirtualNode that = (VirtualNode) o;
        return localNode == that.localNode && hash == that.hash && Objects.equals(nodeUrl, that.nodeUrl);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
