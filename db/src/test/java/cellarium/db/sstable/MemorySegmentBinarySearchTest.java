package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.database.types.IntegerValue;

public class MemorySegmentBinarySearchTest {
    private final static SSTableValueConverter CONVERTER = SSTableValueConverter.INSTANCE;

    @Test
    public void testSingleKey() {
        final long[] offsets = {0};
        final MemorySegment key = CONVERTER.convert(IntegerValue.of(1));
        final MemorySegment indexSegment = MemorySegmentUtils.ARENA_OF_AUTO.allocate(key.byteSize());

        indexSegment.copyFrom(key);

        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                indexSegment,
                offsets,
                key
        );

        Assert.assertEquals(0, foundIndex);
    }

    @Test
    public void testFindExactKey() {
        final TestIndexWrapper index = createIndex(3);

        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                index.index,
                index.offsets,
                index.keys.get(2)
        );

        Assert.assertEquals(2, foundIndex);
    }

    @Test
    public void testFindPositionWithHole() {
        // 0 2 3
        final TestIndexWrapper index = createIndex(4, Set.of(1));

        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                index.index,
                index.offsets,
                CONVERTER.convert(IntegerValue.of(1))
        );

        Assert.assertEquals(-1, foundIndex);
    }

    @Test
    public void testFindPositionWithTwoHoles() {
        // 0 1 2 5
        final TestIndexWrapper index = createIndex(5, Set.of(3, 4));
        final int foundIndex = MemorySegmentUtils.findIndexOfKey(
                index.index,
                index.offsets,
                CONVERTER.convert(IntegerValue.of(4))
        );

        Assert.assertEquals(-3, foundIndex);
    }

    private static TestIndexWrapper createIndex(int keyAmount) {
        return createIndex(keyAmount, Collections.emptySet());
    }

    private static TestIndexWrapper createIndex(int keyAmount, Set<Integer> valueHoles) {
        final long[] offsets = new long[keyAmount - valueHoles.size()];
        final List<MemorySegment> keys = new ArrayList<>();

        long offset = 0;
        int skip = 0;
        for (int i = 0; i < keyAmount; i++) {
            if (valueHoles.contains(i)) {
                skip++;
                continue;
            }

            int index = i - skip;
            offsets[index] = offset;
            final MemorySegment key = CONVERTER.convert(IntegerValue.of(i));
            keys.add(key);

            offset += key.byteSize();
        }

        final MemorySegment indexSegment = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                MemorySegmentUtils.calculateMemorySegmentsSizeBytes(
                        keys
                )
        );

        offset = 0;
        for (MemorySegment key : keys) {
            indexSegment.asSlice(offset, key.byteSize()).copyFrom(key);
            offset += key.byteSize();
        }

        return new TestIndexWrapper(
                offsets,
                indexSegment,
                keys
        );
    }

    private static final class TestIndexWrapper {
        public final long[] offsets;
        public final MemorySegment index;
        public final List<MemorySegment> keys;

        public TestIndexWrapper(long[] offsets, MemorySegment index, List<MemorySegment> keys) {
            this.offsets = offsets;
            this.index = index;
            this.keys = keys;
        }
    }
}
