package cellarium.http.cluster;

public final class ConsistentHashing {
    private static final long MAGIC_NUMBER = 2862933555777941757L;

    private ConsistentHashing() {}

    // Jump Consistent Hash Algorithm
    public static int getNodeIndexForHash(long hash, int nodeAmount) {
        if (nodeAmount == 1) {
            return 0;
        }

        long b = -1;
        long j = 0;

        while (j < nodeAmount) {
            b = j;
            hash = hash * MAGIC_NUMBER + 1;
            j = (long) ((b + 1) * (double) (1L << 31) / ((hash >>> 33) + 1));
        }

        return (int) b;
    }
}
