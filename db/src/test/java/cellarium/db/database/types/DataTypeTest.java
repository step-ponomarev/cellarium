package cellarium.db.database.types;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class DataTypeTest {
    @Test
    public void testDataTypeUniqueId() {
        final DataType[] values = DataType.values();

        final Set<Byte> ids = new HashSet<>();
        for (DataType t : values) {
            ids.add(t.getId());
        }

        Assert.assertEquals(values.length, ids.size());
    }

    @Test
    public void testEachDataTypeSupportedById() {
        final DataType[] values = DataType.values();

        for (DataType t : values) {
            DataType byId = DataType.getById(t.getId());
            Assert.assertNotNull(byId);
        }
    }
}
