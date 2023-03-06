package test.entry;

import cellarium.dao.entry.AbstractEntry;
import cellarium.dao.entry.Entry;

public class NullEntryGeneratorList extends EntryGeneratorList {
    public NullEntryGeneratorList(int count) {
        super(count);
    }

    public NullEntryGeneratorList(int count, String keyPrefix) {
        super(count, keyPrefix, null);
    }

    @Override
    public Entry<String> get(int index) {
        if (index >= count || index < 0) {
            throw new IndexOutOfBoundsException("Available index range: [0, " + count + "], got: " + index);
        }

        return new AbstractEntry<>(keyPrefix + TestUtils.generateKeyByIndex(index), null) {
            @Override
            public String getKey() {
                return super.getKey();
            }

            @Override
            public String getValue() {
                return null;
            }
        };
    }
}
