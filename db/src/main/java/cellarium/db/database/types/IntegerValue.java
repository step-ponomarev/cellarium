package cellarium.db.database.types;

public final class IntegerValue extends AValue<Integer> {
    private IntegerValue(int value) {
        super(value, DataType.INTEGER, Integer.BYTES);
    }

    public static IntegerValue of(int value) {
        return new IntegerValue(value);
    }

    @Override
    public int compareTo(AValue<Integer> o) {
        return this.value - o.getValue();
    }
}
