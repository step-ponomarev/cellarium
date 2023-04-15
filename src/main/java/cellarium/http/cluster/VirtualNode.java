package cellarium.http.cluster;

import one.nio.http.HttpClient;

public final class VirtualNode {
    private final HttpClient httpClient;
    private final int hash;

    public VirtualNode(HttpClient httpClient, int hash) {
        
        this.httpClient = httpClient;
        this.hash = hash;
    }

    public int getHash() {
        return hash;
    }
}
