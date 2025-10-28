package sdmitry.kv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class BitcaskStorageEngineRecoveryTest {
    @Test
    void rebuildsIndexOnStartup(@TempDir Path dir) throws Exception {
        // 1st run: write data
        try (BitcaskStorageEngine eng = new BitcaskStorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng.start();
            eng.put("alpha", "1".getBytes());
            eng.put("beta", "2".getBytes());
        }
        // 2nd run: new engine instance must rebuild index and read values
        try (BitcaskStorageEngine eng2 = new BitcaskStorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng2.start();
            assertArrayEquals("1".getBytes(), eng2.read("alpha"));
            assertArrayEquals("2".getBytes(), eng2.read("beta"));
        }
    }
}
