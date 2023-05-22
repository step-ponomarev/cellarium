package cellarium.db.column;

import java.util.Objects;
import cellarium.db.column.type.ColumnType;

public final class Column {
    public final ColumnType columnType;
    public final String name;
    public final boolean nullable;

    public Column(ColumnType columnType, String name, boolean nullable) {
        if (columnType == null || name == null) {
            throw new NullPointerException("Arguments cannot be null");
        }

        this.columnType = columnType;
        this.name = name;
        this.nullable = nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Column)) return false;
        Column column = (Column) o;
        return name.equals(column.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
