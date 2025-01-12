package cellarium.db.database.table;

import cellarium.db.database.types.AValue;
import cellarium.db.database.types.MemorySegmentValue;

import java.util.Map;

public final class MemorySegmentRow extends TableRow<MemorySegmentValue> {
    public MemorySegmentRow(MemorySegmentValue pk, Map<String, AValue<?>> columns, long sizeBytes) {
        super(pk, columns, sizeBytes);
    }
}
