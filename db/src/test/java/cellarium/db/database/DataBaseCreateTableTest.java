package cellarium.db.database;


import org.junit.Test;

public final class DataBaseCreateTableTest extends ADataBaseTest {
    @Test(expected = NullPointerException.class)
    public void testNullTableName() {
        dataBase.createTable(null, null, null, null);
    }
}
