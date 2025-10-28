package sdmitry.kv.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class Manifest implements AutoCloseable {
    private final Path dir;
    private final Path file;
    private final Deque<Path> newestFirst = new ArrayDeque<>();
    private final List<SstableReader> readers = new ArrayList<>();

    private Manifest(Path dir) {
        this.dir = dir;
        this.file = dir.resolve("MANIFEST.txt");
    }

    static Manifest loadOrCreate(Path dir) throws IOException {
        Files.createDirectories(dir);
        Manifest m = new Manifest(dir);
        if (Files.exists(m.file)) {
            List<String> lines = Files.readAllLines(m.file, StandardCharsets.UTF_8);
            for (String s : lines) if (!s.isBlank()) m.newestFirst.add(dir.resolve(s.trim()));
        } else {
// best-effort scan
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.sst")) {
                for (Path p : ds) m.newestFirst.add(p);
            }
// do not sort strictly; we keep discovery order
            m.storeAtomic();
        }
        m.reopenReaders();
        return m;
    }

    void addHead(Path sst) throws IOException {
        newestFirst.addFirst(sst);
        reopenReaders();
    }

    void replace(List<Path> toRemove, Path merged) throws IOException {
        newestFirst.removeAll(toRemove);
        newestFirst.addFirst(merged);
        reopenReaders();
    }

    List<SstableReader> readersNewestFirst() {
        return Collections.unmodifiableList(readers);
    }

    List<Path> oldestN(int n) {
        List<Path> xs = new ArrayList<>();
        Iterator<Path> it = newestFirst.descendingIterator();
        while (it.hasNext() && xs.size() < n) xs.add(it.next());
        return xs;
    }

    void storeAtomic() throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        List<String> lines = new ArrayList<>();
        for (Path p : newestFirst) lines.add(dir.relativize(p).toString());
        Files.write(tmp, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.READ)) {
            ch.force(true);
        }
        try {
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        }
    }

    private void reopenReaders() throws IOException {
        for (var r : readers)
            try {
                r.close();
            } catch (Exception ignore) {
            }
        readers.clear();
        for (Path p : newestFirst) readers.add(new SstableReader(p));
    }

    @Override
    public void close() throws IOException {
        for (var r : readers) r.close();
    }
}
