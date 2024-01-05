package utils;

import java.lang.foreign.MemorySegment;
import java.util.List;

import cellarium.db.sstable.DataMemorySegmentValue;
import cellarium.db.sstable.IndexMemorySegmentValue;

public final class TestData {
    public final IndexMemorySegmentValue index;
    public final DataMemorySegmentValue data;
    public final List<MemorySegment> keys;

    public TestData(IndexMemorySegmentValue index, List<MemorySegment> keys, DataMemorySegmentValue data) {
        this.index = index;
        this.keys = keys;
        this.data = data;
    }
}
