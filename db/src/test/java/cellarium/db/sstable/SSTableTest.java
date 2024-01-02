package cellarium.db.sstable;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cellarium.db.MemorySegmentUtils;
import cellarium.db.converter.SSTableValueConverter;
import cellarium.db.database.types.IntegerValue;

public class SSTableTest {
    private SSTable ssTable;
    private List<Integer> keys;

    @Before
    public void before() {
        final int amount = 3;
        final List<MemorySegment> keySet = new ArrayList<>(amount);
        final Map<Integer, MemorySegment> values = new HashMap<>(amount, 1);

        keys = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            keySet.add(
                    SSTableValueConverter.INSTANCE.convert(
                            IntegerValue.of(i)
                    )
            );
            values.put(i, SSTableValueConverter.INSTANCE.convert(
                    IntegerValue.of(i)
            ));
            keys.add(i);
        }

        final MemorySegment data = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                MemorySegmentUtils.calculateMemorySegmentsSizeBytes(keySet) + MemorySegmentUtils.calculateMemorySegmentsSizeBytes(values.values())
        );

        final MemorySegment index = MemorySegmentUtils.ARENA_OF_AUTO.allocate(
                MemorySegmentUtils.calculateMemorySegmentsSizeBytes(keySet)
        );

        final long[] offsets = new long[amount];
        long dataOffset = 0;
        long indexOffset = 0;
        for (int i = 0; i < amount; i++) {
            final MemorySegment key = keySet.get(i);

            data.asSlice(dataOffset, key.byteSize()).copyFrom(key);
            dataOffset += key.byteSize();

            final MemorySegment value = values.get(i);
            data.asSlice(dataOffset, value.byteSize()).copyFrom(value);
            dataOffset += value.byteSize();

            offsets[i] = indexOffset;
            index.asSlice(indexOffset, key.byteSize()).copyFrom(key);
            indexOffset += key.byteSize();
        }

        ssTable = new SSTable(
                index,
                offsets,
                data
        );
    }

    @After
    public void after() {
        System.out.println("HERE");
    }

    @Test
    public void test() {
        MemorySegment dataRange = ssTable.getDataRange(
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(1)),
                SSTableValueConverter.INSTANCE.convert(IntegerValue.of(1))
        );
    }


}
