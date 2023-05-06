package cellarium;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

public final class DiskUtils {
    private DiskUtils() {}

    public static void removeDir(Path path) throws IOException {
        try (Stream<Path> ssTableFiles = Files.walk(path)) {
            final Iterator<Path> filesToRemove = ssTableFiles.sorted(Comparator.reverseOrder()).iterator();
            while (filesToRemove.hasNext()) {
                Files.delete(filesToRemove.next());
            }
        }
    }

    public static long gerDirSizeBytes(final Path path) throws IOException {
        return Files.walk(path).mapToLong(f -> {
            try {
                return Files.size(f);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }).sum();
    }

    public static boolean isDirEmpty(Path dir) throws IOException {
        if (Files.notExists(dir)) {
            throw new IllegalStateException("Dir is not exists: " + dir);
        }

        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("Path is not dir: " + dir);
        }

        try (DirectoryStream<Path> directory = Files.newDirectoryStream(dir)) {
            return !directory.iterator().hasNext();
        }
    }
}
