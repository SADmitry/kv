package sdmitry.kv;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface KeyValueStore extends Closeable {
    void start() throws IOException;
    void put(String key, byte[] value) throws IOException;
    int batchPut(List<Map.Entry<String, byte[]>> items) throws IOException;
    byte[] read(String key) throws IOException;
    List<Map.Entry<String, byte[]>> readKeyRange(String startInclusive, String endInclusive, int limit) throws IOException;
    void delete(String key) throws IOException;
    long compact() throws IOException;
}
