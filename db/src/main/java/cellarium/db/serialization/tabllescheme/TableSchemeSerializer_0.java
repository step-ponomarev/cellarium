package cellarium.db.serialization.tabllescheme;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.table.TableScheme;
import cellarium.db.database.types.DataType;
import cellarium.db.serialization.DataSerializer;
import cellarium.db.serialization.columnscheme.ColumnSchemeSerializer;

final class TableSchemeSerializer_0 implements DataSerializer<TableScheme> {
    static final TableSchemeSerializer_0 INSTANCE = new TableSchemeSerializer_0();
    private static final ColumnSchemeSerializer COLUMN_SCHEME_SERIALIZER = ColumnSchemeSerializer.INSTANCE;

    private TableSchemeSerializer_0() {}

    @Override
    public TableScheme read(DataInputStream is) throws IOException {
        final ColumnScheme pk = COLUMN_SCHEME_SERIALIZER.read(is);

        int columnCount = is.readInt();
        final Map<String, DataType> columnTypes = new HashMap<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            final String columnName = is.readUTF();
            final DataType dataType = DataType.getById(is.readByte());
            columnTypes.put(columnName, dataType);
        }

        columnCount = is.readInt();
        final List<ColumnScheme> scheme = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            scheme.add(COLUMN_SCHEME_SERIALIZER.read(is));
        }

        return new TableScheme(pk, columnTypes, scheme, is.readInt());
    }

    @Override
    public void write(TableScheme obj, DataOutputStream os) throws IOException {
        COLUMN_SCHEME_SERIALIZER.write(obj.getPrimaryKey(), os);

        final Map<String, DataType> columnTypes = obj.getColumnTypes();
        os.writeInt(columnTypes.size());

        for (Map.Entry<String, DataType> entry : columnTypes.entrySet()) {
            os.writeUTF(entry.getKey());
            os.writeByte(entry.getValue().getId());
        }

        final List<ColumnScheme> scheme = obj.getScheme();
        os.writeInt(scheme.size());

        for (ColumnScheme cs : scheme) {
            COLUMN_SCHEME_SERIALIZER.write(cs, os);
        }

        os.writeInt(obj.getVersion());
    }
}
