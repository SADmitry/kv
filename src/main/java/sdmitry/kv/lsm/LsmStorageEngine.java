package sdmitry.kv.lsm;

import sdmitry.kv.KeyValueStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Minimal LSM engine: WAL + MemTable + SSTables + manual compaction.
 */
public final class LsmStorageEngine implements KeyValueStore {
    private final Path dir;
    private final long memtableMaxBytes;
    private final int indexEvery;

    private Wal wal;
    private final ConcurrentSkipListMap<String, byte[]> mem = new ConcurrentSkipListMap<>();
    private Manifest manifest;

    private static final byte[] TOMBSTONE = new byte[0];

    /**
     * @param dir               data folder
     * @param memtableMaxBytes  flesh gate for MemTable (bytes)
     */
    public LsmStorageEngine(Path dir, long memtableMaxBytes, long ignoredFsyncMs) {
        this.dir = Objects.requireNonNull(dir, "dir");
        this.memtableMaxBytes = memtableMaxBytes > 0 ? memtableMaxBytes : (16L << 20); // 16 MiB
        this.indexEvery = 64;
    }

    @Override
    public void start() throws IOException {
        Files.createDirectories(dir);
        this.wal = new Wal(dir.resolve("wal.log"));
        this.manifest = Manifest.loadOrCreate(dir);
        // Recovery from WAL
        wal.replay((op, k, v) -> {
            if (op == Wal.PUT) mem.put(k, v);
            else mem.put(k, TOMBSTONE);
        });
    }

    @Override
    public void close() throws IOException {
        if (!mem.isEmpty()) flushMemTable();
        if (wal != null) wal.close();
        if (manifest != null) manifest.close();
    }

    @Override
    public void put(String key, byte[] value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        wal.appendPut(key, value);
        mem.put(key, value);
        maybeFlush();
    }

    @Override
    public int batchPut(List<Map.Entry<String, byte[]>> items) throws IOException {
        if (items == null || items.isEmpty()) return 0;
        for (var e : items) wal.appendPut(e.getKey(), e.getValue());
        for (var e : items) mem.put(e.getKey(), e.getValue());
        maybeFlush();
        return items.size();
    }

    @Override
    public void delete(String key) throws IOException {
        Objects.requireNonNull(key, "key");
        wal.appendDelete(key);
        mem.put(key, TOMBSTONE);
        maybeFlush();
    }

    // ── read path ───────────────────────────────────────────────────────────────

    @Override
    public byte[] read(String key) throws IOException {
        byte[] v = mem.get(key);
        if (v != null) return v == TOMBSTONE ? null : v;

        // Looking SST newer to older
        for (var reader : manifest.readersNewestFirst()) {
            v = reader.get(key);
            if (v != null) return Arrays.equals(v, TOMBSTONE) ? null : v;
        }
        return null;
    }

    @Override
    public List<Map.Entry<String, byte[]>> readKeyRange(String start, String end, int limit) throws IOException {
        if (limit <= 0) return List.of();

        List<PeekingIter> sources = new ArrayList<>();
        sources.add(PeekingIter.from(mem.subMap(start, true, end, true).entrySet().iterator()));
        for (var r : manifest.readersNewestFirst()) {
            sources.add(PeekingIter.from(r.iterator(start, end)));
        }

        List<Map.Entry<String, byte[]>> out = new ArrayList<>(Math.min(limit, 1024));
        String lastEmittedKey = null;

        while (out.size() < limit) {
            int bestIdx = -1;
            String bestKey = null;
            for (int i = 0; i < sources.size(); i++) {
                var it = sources.get(i);
                if (!it.has()) continue;
                var k = it.peekKey();
                if (k == null) continue;
                if (bestKey == null || k.compareTo(bestKey) < 0 || (k.equals(bestKey) && i < bestIdx)) {
                    bestKey = k; bestIdx = i;
                }
            }
            if (bestIdx < 0) break; // всё исчерпано

            var chosen = sources.get(bestIdx).pop();

            for (int i = 0; i < sources.size(); i++) {
                while (sources.get(i).has() && Objects.equals(sources.get(i).peekKey(), chosen.getKey())) {
                    sources.get(i).pop();
                }
            }

            if (!Arrays.equals(chosen.getValue(), TOMBSTONE)
                    && (lastEmittedKey == null || !lastEmittedKey.equals(chosen.getKey()))) {
                out.add(Map.entry(chosen.getKey(), chosen.getValue()));
                lastEmittedKey = chosen.getKey();
            }
        }
        return out;
    }

    @Override
    public long compact() throws IOException {
        // Слить несколько самых старых SST в один (простой size-tiered подход)
        return Compactor.runOnce(dir, manifest, 3, indexEvery);
    }

    private void maybeFlush() throws IOException {
        if (approxMemBytes() >= memtableMaxBytes) flushMemTable();
    }

    private long approxMemBytes() {
        long n = 0;
        for (var e : mem.entrySet()) {
            n += /* UTF-8 worst-case */ e.getKey().getBytes(StandardCharsets.UTF_8).length;
            if (e.getValue() != null) n += e.getValue().length;
        }
        return n;
    }

    /**
     * Snapshot MemTable to SST, rotate WAL, update MANIFEST.
     * */
    private void flushMemTable() throws IOException {
        if (mem.isEmpty()) return;

        List<Map.Entry<String, byte[]>> snapshot = new ArrayList<>(mem.entrySet());
        mem.clear();

        Path sst = SstableWriter.write(dir, snapshot, indexEvery);

        wal.rotate();

        manifest.addHead(sst);
        manifest.storeAtomic();
    }

    private static final class PeekingIter {
        private Map.Entry<String, byte[]> next;
        private final Iterator<Map.Entry<String, byte[]>> it;

        private PeekingIter(Iterator<Map.Entry<String, byte[]>> it) {
            this.it = it;
            advance();
        }

        static PeekingIter from(Iterator<Map.Entry<String, byte[]>> it) { return new PeekingIter(it); }

        boolean has() { return next != null; }

        String peekKey() { return next == null ? null : next.getKey(); }

        Map.Entry<String, byte[]> pop() {
            var n = next;
            advance();
            return n;
        }

        private void advance() {
            next = it.hasNext() ? it.next() : null;
        }
    }
}

