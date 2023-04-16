package cellarium.http.cluster;

import java.util.Objects;

final class VirtualNode extends Node {
    private final int hash;

    public VirtualNode(String nodeUrl, boolean localNode, int hash) {
        super(nodeUrl, localNode);
        this.hash = hash;
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
