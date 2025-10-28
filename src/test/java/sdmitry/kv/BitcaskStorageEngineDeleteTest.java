package sdmitry.kv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class BitcaskStorageEngineDeleteTest {
    @Test
    void deleteRemovesKey(@TempDir Path dir) throws Exception {
        try (BitcaskStorageEngine eng = new BitcaskStorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng.start();
            eng.put("k", "v".getBytes());
            eng.delete("k");
            assertNull(eng.read("k"));
        }
    }
}
