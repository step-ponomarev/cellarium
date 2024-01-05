package utils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.sstable.DataMemorySegmentValue;
import cellarium.db.sstable.IndexMemorySegmentValue;

public final class SSTableTestUtils {
    private SSTableTestUtils() {
    }

    public static TestData mockIntListData(int dataAmount) {
        return mockIntListData(dataAmount, Collections.emptySet());
    }

    public static TestData mockIntListData(int dataAmount, Set<Integer> keyHoles) {
        final List<MemorySegment> values = new ArrayList<>(dataAmount - keyHoles.size());

        for (int i = 0; i < dataAmount; i++) {
            if (keyHoles.contains(i)) {
                continue;
            }

            values.add(
                    SSTableValueConverter.INSTANCE.convert(IntegerValue.of(i))
            );
        }

        final MemorySegment indexSegment = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                (long) values.size() * Long.BYTES
        );

        final MemorySegment dataSegment = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                MemorySegmentUtils.calculateMemorySegmentsSizeBytes(values)
        );

        long offset = 0;
        long indexOffset = 0;
        for (MemorySegment key : values) {
            indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, offset);
            indexOffset += Long.BYTES;
            
            dataSegment.asSlice(offset, key.byteSize()).copyFrom(key);
            offset += key.byteSize();
        }

        return new TestData(
                new IndexMemorySegmentValue(indexSegment),
                values,
                new DataMemorySegmentValue(dataSegment)
        );
    }
}
