package cellarium.db.database;

import org.junit.Before;

abstract class ADataBaseTest {
    protected DataBase dataBase;

    @Before
    public void init() {
        this.dataBase = new CellariumDB();
    }
}
