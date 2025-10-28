package sdmitry.kv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class BitcaskStorageEnginePutGetTest {
    @Test
    void putThenGetReturnsValue(@TempDir Path dir) throws Exception {
        try (BitcaskStorageEngine eng = new BitcaskStorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng.start();
            eng.put("foo", "bar".getBytes());
            assertArrayEquals("bar".getBytes(), eng.read("foo"));
        }
    }
}
