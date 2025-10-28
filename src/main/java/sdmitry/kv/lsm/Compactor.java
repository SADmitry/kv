package sdmitry.kv.lsm;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class Compactor {
    static long runOnce(Path dir, Manifest manifest, int pickCount, int indexEvery) throws IOException {
        List<Path> olds = manifest.oldestN(pickCount);
        if (olds.size() <= 1) return 0L;

        // Create readers in newest-to-oldest order for tie-breaking by recency
        List<SstableReader> readers = new ArrayList<>();
        for (Path p : olds) readers.add(new SstableReader(p));

        // Build iterators over full keyspace
        List<Iterator<Map.Entry<String, byte[]>>> iters = new ArrayList<>();
        for (SstableReader r : readers) iters.add(r.iterator("", "\uffff"));

        // k-way merge with newest-wins on duplicate keys
        Map<String, byte[]> merged = new TreeMap<>();
        for (int i = iters.size() - 1; i >= 0; i--) { // oldest first, then overwritten by newer
            var it = iters.get(i);
            while (it.hasNext()) {
                var e = it.next();
                merged.put(e.getKey(), e.getValue());
            }
        }

        // write new SST
        List<Map.Entry<String, byte[]>> entries = new ArrayList<>(merged.entrySet());
        Path newSst = SstableWriter.write(dir, entries, indexEvery);

        // update manifest and delete old files
        manifest.replace(olds, newSst);
        manifest.storeAtomic();
        long reclaimed = 0;
        for (Path p : olds) {
            try {
                reclaimed += Files.size(p);
                Files.deleteIfExists(p);
            } catch (Exception ignore) {
            }
        }
        for (SstableReader r : readers)
            try {
                r.close();
            } catch (Exception ignore) {
            }
        return reclaimed;
    }
}
