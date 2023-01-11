package test.entry;

import java.util.AbstractList;
import cellarium.entry.AbstractEntry;
import cellarium.entry.Entry;

public class EntryGeneratorList extends AbstractList<Entry<String>> {
    protected final int count;
    protected final String keyPrefix;
    protected final String valuePrefix;

    public EntryGeneratorList(int count) {
        this.count = count;
        this.keyPrefix = "KEY_";
        this.valuePrefix = "VALUE_";
    }

    public EntryGeneratorList(int count, String keyPrefix, String valuePrefix) {
        this.keyPrefix = keyPrefix;
        this.valuePrefix = valuePrefix;
        this.count = count;
    }

    @Override
    public Entry<String> get(int index) {
        if (index >= count || index < 0) {
            throw new IndexOutOfBoundsException("Available index range: [0, " + count + "], got: " + index);
        }

        final String key = Utils.generateKeyByIndex(index);
        return new AbstractEntry<>(keyPrefix + key, valuePrefix + key) {
            @Override
            public String getKey() {
                return super.getKey();
            }

            @Override
            public String getValue() {
                return super.getValue();
            }
        };
    }

    @Override
    public int size() {
        return count;
    }
}
