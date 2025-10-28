package sdmitry.kv.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;

public class Wal implements AutoCloseable {
    static final byte PUT = 0, DEL = 1;

    interface Replay {
        void apply(byte op, String key, byte[] value) throws IOException;
    }

    private final Path active;
    private FileChannel ch;

    Wal(Path active) throws IOException {
        this.active = active;
        this.ch = FileChannel.open(active, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    void appendPut(String key, byte[] value) throws IOException {
        append((byte) 0, key, value);
    }

    void appendDelete(String key) throws IOException {
        append((byte) 1, key, new byte[0]);
    }

    private synchronized void append(byte op, String key, byte[] value) throws IOException {
        byte[] kb = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 c = new CRC32();
        c.update(op);
        c.update(int4(kb.length));
        c.update(int4(value.length));
        c.update(kb);
        c.update(value);
        int crc = (int) c.getValue();
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 1 + 4 + 4 + kb.length + value.length);
        buf.putInt(0x57414C31).putInt(crc).put(op).putInt(kb.length).putInt(value.length).put(kb).put(value).flip();
        ch.write(buf);
// caller decides when to fsync (we keep it simple here)
    }

    void replay(Replay consumer) throws IOException {
        if (!Files.exists(active)) return;
        try (FileChannel rch = FileChannel.open(active, StandardOpenOption.READ)) {
            long pos = 0;
            ByteBuffer head = ByteBuffer.allocate(4 + 4 + 1 + 4 + 4);
            while (pos < rch.size()) {
                head.clear();
                int n = rch.read(head, pos);
                if (n < head.capacity()) break;
                head.flip();
                int magic = head.getInt();
                if (magic != 0x57414C31) break;
                int crcStored = head.getInt();
                byte op = head.get();
                int klen = head.getInt();
                int vlen = head.getInt();
                ByteBuffer kb = ByteBuffer.allocate(klen);
                ByteBuffer vb = ByteBuffer.allocate(vlen);
                int nk = rch.read(kb, pos + head.capacity());
                int nv = rch.read(vb, pos + head.capacity() + klen);
                if (nk < klen || nv < vlen) break;
                kb.flip();
                vb.flip();
                byte[] k = new byte[klen];
                kb.get(k);
                byte[] v = new byte[vlen];
                vb.get(v);
                CRC32 c = new CRC32();
                c.update(op);
                c.update(int4(klen));
                c.update(int4(vlen));
                c.update(k);
                c.update(v);
                if ((int) c.getValue() != crcStored) break; // torn tail
                consumer.apply(op, new String(k, java.nio.charset.StandardCharsets.UTF_8), v);
                pos = pos + head.capacity() + klen + vlen;
            }
        }
    }

    void rotate() throws IOException {
// Close and atomically rename current WAL to a timestamped file; start new one
        ch.force(true);
        ch.close();
        String stamp = String.valueOf(System.currentTimeMillis());
        Path rotated = active.getParent().resolve("wal-" + stamp + ".log");
        try {
            Files.move(active, rotated, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(active, rotated);
        }
// fsync dir for durability
        try (var dirCh = FileChannel.open(active.getParent(), StandardOpenOption.READ)) {
            dirCh.force(true);
        }
        ch = FileChannel.open(active, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    @Override
    public void close() throws IOException {
        if (ch != null) ch.close();
    }

    private static byte[] int4(int x) {
        return ByteBuffer.allocate(4).putInt(x).array();
    }
}
