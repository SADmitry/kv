package sdmitry.kv.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class SstableReader implements AutoCloseable {
    static final byte[] TOMBSTONE = new byte[0];

    private final Path path;
    private final FileChannel ch;
    private final List<Idx> index = new ArrayList<>();
    private long dataEnd;

    SstableReader(Path path) throws IOException {
        this.path = path;
        this.ch = FileChannel.open(path, StandardOpenOption.READ);
        long size = ch.size();
        // read footer
        ByteBuffer f = ByteBuffer.allocate(4 + 4 + 8 + 4);
        ch.read(f, size - f.capacity());
        f.flip();
        int magic = f.getInt();
        if (magic != 0x53535431) { /* empty or old */
            ch.close();
            throw new IOException("bad sst footer");
        }
        int count = f.getInt();
        long indexStart = f.getLong(); /*crc*/
        f.getInt();
        this.dataEnd = indexStart;
        long pos = indexStart;
        for (int i = 0; i < count; i++) {
            ByteBuffer h = ByteBuffer.allocate(4);
            ch.read(h, pos);
            h.flip();
            int klen = h.getInt();
            pos += 4;
            ByteBuffer kb = ByteBuffer.allocate(klen);
            ch.read(kb, pos);
            kb.flip();
            pos += klen;
            ByteBuffer off = ByteBuffer.allocate(8);
            ch.read(off, pos);
            off.flip();
            pos += 8;
            String key = new String(kb.array(), StandardCharsets.UTF_8);
            long offv = off.getLong();
            index.add(new Idx(key, offv));
        }
    }

    byte[] get(String key) throws IOException {
        if (index.isEmpty()) return null;
        int lo = 0, hi = index.size() - 1, best = -1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            int cmp = index.get(mid).key.compareTo(key);
            if (cmp <= 0) {
                best = mid;
                lo = mid + 1;
            } else hi = mid - 1;
        }
        long startOff = best >= 0 ? index.get(best).off : 0;
        return scanForKey(startOff, key);
    }

    private byte[] scanForKey(long startOff, String target) throws IOException {
        long pos = startOff;
        while (pos < dataEnd) {
            ByteBuffer h = ByteBuffer.allocate(4 + 4);
            int n = ch.read(h, pos);
            if (n < h.capacity()) break;
            h.flip();
            int klen = h.getInt();
            int vlen = h.getInt();
            pos += h.capacity();
            ByteBuffer kb = ByteBuffer.allocate(klen);
            ch.read(kb, pos);
            kb.flip();
            pos += klen;
            String key = new String(kb.array(), StandardCharsets.UTF_8);
            if (key.compareTo(target) > 0) return null; // since sorted, no more matches
            ByteBuffer vb = ByteBuffer.allocate(vlen);
            ch.read(vb, pos);
            pos += vlen;
            if (key.equals(target)) return vb.array();
        }
        return null;
    }

    Iterator<Map.Entry<String, byte[]>> iterator(String start, String end) throws IOException {
        // find closest index entry >= start
        int lo = 0, hi = index.size() - 1, first = 0;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            int cmp = index.get(mid).key.compareTo(start);
            if (cmp < 0) lo = mid + 1;
            else {
                first = mid;
                hi = mid - 1;
            }
        }
        long pos = (index.isEmpty() ? 0 : (first < index.size() ? index.get(first).off : index.get(index.size() - 1).off));
        return new Iter(pos, end);
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    private record Idx(String key, long off) {
    }

    private final class Iter implements Iterator<Map.Entry<String, byte[]>> {
        private long pos;
        private final String end;
        private Map.Entry<String, byte[]> next;

        Iter(long startOff, String end) {
            this.pos = startOff;
            this.end = end;
            advance();
        }

        private void advance() {
            try {
                while (pos < dataEnd) {
                    ByteBuffer h = ByteBuffer.allocate(4 + 4);
                    int n = ch.read(h, pos);
                    if (n < h.capacity()) {
                        next = null;
                        return;
                    }
                    h.flip();
                    int klen = h.getInt();
                    int vlen = h.getInt();
                    pos += h.capacity();
                    ByteBuffer kb = ByteBuffer.allocate(klen);
                    ch.read(kb, pos);
                    pos += klen;
                    String k = new String(kb.array(), StandardCharsets.UTF_8);
                    ByteBuffer vb = ByteBuffer.allocate(vlen);
                    ch.read(vb, pos);
                    pos += vlen;
                    byte[] v = vb.array();
                    if (k.compareTo(end) > 0) {
                        next = null;
                        return;
                    }
                    next = Map.entry(k, v);
                    return;
                }
                next = null;
            } catch (IOException e) {
                next = null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Map.Entry<String, byte[]> next() {
            var n = next;
            advance();
            return n;
        }
    }
}
