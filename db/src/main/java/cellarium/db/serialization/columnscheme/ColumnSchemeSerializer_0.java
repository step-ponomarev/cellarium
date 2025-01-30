package cellarium.db.serialization.columnscheme;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cellarium.db.database.table.ColumnScheme;
import cellarium.db.database.types.DataType;
import cellarium.db.serialization.DataSerializer;

final class ColumnSchemeSerializer_0 implements DataSerializer<ColumnScheme> {
    static final ColumnSchemeSerializer_0 INSTANCE = new ColumnSchemeSerializer_0();

    private ColumnSchemeSerializer_0() {}

    @Override
    public ColumnScheme read(DataInputStream is) throws IOException {
        final String name = is.readUTF();
        final DataType dataType = DataType.getById(is.readByte());

        return new ColumnScheme(name, dataType);
    }

    @Override
    public void write(ColumnScheme obj, DataOutputStream os) throws IOException {
        os.writeUTF(obj.getName());
        os.writeByte(obj.getType().getId());
    }
}
