package sdmitry.kv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class BitcaskStorageEngineCompactionTest {
    @Test
    void compactionRewritesLiveSetAndReclaimsSpace(@TempDir Path dir) throws Exception {
        // we give it a small gap
        try (BitcaskStorageEngine eng = new BitcaskStorageEngine(dir, 64 * 1024, 0)) {
            eng.start();

            // generate garbage
            eng.put("k1", "v1".getBytes());
            eng.put("k1", "v2".getBytes()); // old version, must disapear
            eng.put("k2", "x".getBytes());
            eng.delete("k2"); // tombstone

            // before
            long before = Files.walk(dir)
                    .filter(p -> p.toString().endsWith(".seg"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (Exception e) {
                            return 0L;
                        }
                    })
                    .sum();

            long reclaimed = eng.compact();

            // after
            long after = Files.walk(dir)
                    .filter(p -> p.toString().endsWith(".seg"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (Exception e) {
                            return 0L;
                        }
                    })
                    .sum();

            // check live set
            assertArrayEquals("v2".getBytes(), eng.read("k1"), "k1 must keep latest value");
            assertNull(eng.read("k2"), "k2 must be deleted");

            // and what actually happened
            assertTrue(reclaimed >= 0, "reclaimed should be non-negative");
            assertTrue(after <= before, "size after compaction should not exceed size before");
        }
    }
}
