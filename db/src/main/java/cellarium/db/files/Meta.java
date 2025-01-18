package cellarium.db.files;

import java.io.Serializable;
import java.util.List;

public final class Meta implements Serializable {
    private static final long serialVersionUID = 1L;

    public final List<String> tables;

    public Meta(List<String> tables) {
        this.tables = tables;
    }
}
