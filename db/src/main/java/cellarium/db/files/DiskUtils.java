package cellarium.db.files;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Stack;
import java.util.stream.Stream;

public final class DiskUtils {
    public static void removeFile(Path path) throws IOException {
        if (Files.notExists(path)) {
            return;
        }

        if (!Files.isDirectory(path)) {
            Files.delete(path);
            return;
        }

        final Stack<Path> dirs = new Stack<>();
        dirs.add(path);

        while (!dirs.isEmpty()) {
            final Path currDir = dirs.pop();

            final boolean emptyDir;
            try (final Stream<Path> list = Files.list(currDir)) {
                final Iterator<Path> iterator = list.iterator();

                emptyDir = !iterator.hasNext();
                if (!emptyDir) {
                    dirs.add(currDir);
                } else {
                    Files.delete(currDir);
                }

                while (iterator.hasNext()) {
                    final Path next = iterator.next();
                    if (Files.isDirectory(next)) {
                        dirs.add(next);
                    } else {
                        Files.delete(next);
                    }
                }
            }


        }
    }

    private DiskUtils() {}
}
