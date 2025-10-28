package sdmitry.kv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineRecoveryTest {
    @Test
    void rebuildsIndexOnStartup(@TempDir Path dir) throws Exception {
        // 1st run: write data
        try (StorageEngine eng = new StorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng.start();
            eng.put("alpha", "1".getBytes());
            eng.put("beta", "2".getBytes());
        }
        // 2nd run: new engine instance must rebuild index and read values
        try (StorageEngine eng2 = new StorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng2.start();
            assertArrayEquals("1".getBytes(), eng2.read("alpha"));
            assertArrayEquals("2".getBytes(), eng2.read("beta"));
        }
    }
}
