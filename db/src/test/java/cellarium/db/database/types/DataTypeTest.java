package cellarium.db.database.types;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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

    @Test
    public void testEquals() {
        final DataType[] values = DataType.values();

        for (DataType t : values) {
            switch (t) {
                case BOOLEAN:
                    testBooleanValuesEquals();
                    break;
                case STRING:
                    testStringValuesEquals();
                    break;
                case LONG:
                    testLongValueEquals();
                    break;
                case INTEGER:
                    testIntegerValueEquals();
                    break;
                default:
                    throw new IllegalStateException(STR."Unsupported test case for \{t.name()}");
            }
        }
    }

    private void testIntegerValueEquals() {
        for (int i = -1000; i < 1000; i++) {
            Assert.assertEquals(IntegerValue.of(i), IntegerValue.of(i));

            int notEquals;
            while ((notEquals = ThreadLocalRandom.current().nextInt()) == i) {
                notEquals = ThreadLocalRandom.current().nextInt();
            }

            Assert.assertNotEquals(IntegerValue.of(notEquals), IntegerValue.of(i));
            Assert.assertNotEquals(IntegerValue.of(i), IntegerValue.of(notEquals));
        }
    }

    private void testLongValueEquals() {
        for (long i = -10000; i < 10000; i++) {
            Assert.assertEquals(LongValue.of(i), LongValue.of(i));

            long notEquals;
            while ((notEquals = ThreadLocalRandom.current().nextInt()) == i) {
                notEquals = ThreadLocalRandom.current().nextInt();
            }

            Assert.assertNotEquals(LongValue.of(notEquals), LongValue.of(i));
            Assert.assertNotEquals(LongValue.of(i), LongValue.of(notEquals));
        }
    }

    private void testStringValuesEquals() {
        for (int i = 0; i < 100; i++) {
            byte[] randomBytes = new byte[i + 1]; // length is bounded by 7
            new Random().nextBytes(randomBytes);
            final String generatedString = new String(randomBytes, StandardCharsets.UTF_8);

            Assert.assertEquals(StringValue.of(generatedString), (StringValue.of(generatedString)));


            randomBytes = new byte[new Random().nextInt(i + 1, 130)]; // length is bounded by 7
            new Random().nextBytes(randomBytes);
            String notEquals = new String(randomBytes, StandardCharsets.UTF_8);
            Assert.assertNotEquals(generatedString, notEquals);

            Assert.assertNotEquals(StringValue.of(notEquals), StringValue.of(generatedString));
            Assert.assertNotEquals(StringValue.of(generatedString), StringValue.of(notEquals));
        }
    }

    private void testBooleanValuesEquals() {
        Assert.assertEquals(BooleanValue.of(true), BooleanValue.of(true));
        Assert.assertNotEquals(BooleanValue.of(true), BooleanValue.of(false));
        Assert.assertNotEquals(BooleanValue.of(false), BooleanValue.of(true));
    }
}
