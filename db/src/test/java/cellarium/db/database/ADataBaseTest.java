package cellarium.db.database;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;

import cellarium.db.config.CellariumConfig;
import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.Row;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.AValue;
import cellarium.db.database.types.BooleanValue;
import cellarium.db.database.types.DataType;
import cellarium.db.database.types.IntegerValue;
import cellarium.db.database.types.LongValue;
import cellarium.db.database.types.StringValue;
import cellarium.db.files.DiskUtils;

abstract class ADataBaseTest {
    private static final Path RESOURCES_PATH = Path.of("src/test/resources");
    private static final String TABLE_NAME = "user";
    private static final TableScheme SCHEME;

    protected static final String COLUMN_ID = "id";
    protected static final String COLUMN_NAME = "name";
    protected static final String COLUMN_AGE = "age";
    protected static final String COLUMN_GENDER = "gender";
    protected static final String COLUMN_BIRTHDAY = "birthDay";

    static {
        final ColumnScheme id = new ColumnScheme(COLUMN_ID, DataType.STRING);
        final Map<String, DataType> columns = Map.of(
                COLUMN_NAME, DataType.STRING,
                COLUMN_AGE, DataType.INTEGER,
                COLUMN_GENDER, DataType.BOOLEAN,
                COLUMN_BIRTHDAY, DataType.LONG
        );
        final List<ColumnScheme> scheme = columns.entrySet()
                .stream()
                .map(e -> new ColumnScheme(e.getKey(), e.getValue())).toList();
        SCHEME = new TableScheme(id, columns, scheme, 0);
    }

    protected DataBase dataBase;

    @Before
    public void init() {
        try {
            this.dataBase = new CellariumDB(new CellariumConfig(getMaxBytes(), RESOURCES_PATH.resolve("cellarium_db")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected long getMaxBytes() {
        return Integer.MAX_VALUE;
    }

    @After
    public void after() throws Exception {
        close();
    }

    public void close() throws IOException {
        if (this.dataBase != null) {
            this.dataBase.close();
            DiskUtils.removeFile(RESOURCES_PATH.resolve("cellarium_db"));
        }
    }

    protected Iterator<Row<AValue<?>, AValue<?>>> select(Integer from, Integer to, Set<String> filterCounts) {
        final StringValue fromStrValue = from == null ? null : StringValue.of(String.valueOf(from));
        final StringValue toStrValue = to == null ? null : StringValue.of(String.valueOf(to));

        return dataBase.getRange(TABLE_NAME, fromStrValue, toStrValue, filterCounts);
    }

    protected void createTable() {
        dataBase.createTable(TABLE_NAME, SCHEME.getPrimaryKey(), SCHEME.getScheme());
    }

    protected void delete(int id) {
        dataBase.deleteByPk(TABLE_NAME, StringValue.of(String.valueOf(id)));
    }

    protected Map<String, AValue<?>> insertRow(int id, String name, int age, boolean gender, long birthDay) {
        final Map<String, AValue<?>> addedValues = Map.of(
                COLUMN_ID,
                StringValue.of(String.valueOf(id)),
                COLUMN_NAME,
                StringValue.of(name),
                COLUMN_AGE,
                IntegerValue.of(age),
                COLUMN_GENDER,
                BooleanValue.of(gender),
                COLUMN_BIRTHDAY,
                LongValue.of(birthDay)
        );

        dataBase.insert(TABLE_NAME, addedValues);

        return addedValues;
    }
}
