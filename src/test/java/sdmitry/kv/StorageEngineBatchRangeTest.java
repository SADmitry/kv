package sdmitry.kv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineBatchRangeTest {
    @Test
    void batchPutThenRangeReadsLexicographically(@TempDir Path dir) throws Exception {
        try (StorageEngine eng = new StorageEngine(dir, 4 * 1024 * 1024, 0)) {
            eng.start();
            int written = eng.batchPut(List.of(
                    Map.entry("a", "b".getBytes()),
                    Map.entry("foo", "BAR2".getBytes()),
                    Map.entry("z", "last".getBytes())
            ));
            assertEquals(3, written);

            var items = eng.readKeyRange("a", "g", 10);
            assertEquals(2, items.size());
            assertEquals("a", items.get(0).getKey());
            assertArrayEquals("b".getBytes(), items.get(0).getValue());
            assertEquals("foo", items.get(1).getKey());
            assertArrayEquals("BAR2".getBytes(), items.get(1).getValue());
        }
    }
}
