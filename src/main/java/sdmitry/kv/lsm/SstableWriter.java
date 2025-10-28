package sdmitry.kv.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class SstableWriter {
    static Path write(Path dir, List<Map.Entry<String, byte[]>> entries, int indexEvery) throws IOException {
        entries.sort(Map.Entry.comparingByKey());
        String name = String.format("%020d.sst", System.currentTimeMillis());
        Path tmp = dir.resolve(name + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            List<IndexEntry> index = new ArrayList<>();
            long off = 0;
            int i = 0;
            for (var e : entries) {
                byte[] kb = e.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] vb = e.getValue();
                ByteBuffer rec = ByteBuffer.allocate(4 + 4 + kb.length + vb.length);
                rec.putInt(kb.length).putInt(vb.length).put(kb).put(vb).flip();
                ch.write(rec, off);
                if (i % indexEvery == 0) index.add(new IndexEntry(e.getKey(), off));
                off += rec.capacity();
                i++;
            }
            // write index block
            long indexStart = off;
            int indexCount = index.size();
            for (IndexEntry ie : index) {
                byte[] kb = ie.key.getBytes(StandardCharsets.UTF_8);
                ByteBuffer ib = ByteBuffer.allocate(4 + kb.length + 8);
                ib.putInt(kb.length).put(kb).putLong(ie.offset).flip();
                ch.write(ib, off);
                off += ib.capacity();
            }
            // footer: magic 'SST1'(4) indexCount(4) indexStart(8) crc(4)
            ByteBuffer footer = ByteBuffer.allocate(4 + 4 + 8 + 4);
            footer.putInt(0x53535431).putInt(indexCount).putLong(indexStart).putInt(0);
            footer.flip();
            ch.write(footer, off);
            off += footer.capacity();
            ch.force(true);
        }
        Path sst = dir.resolve(name);
        try {
            Files.move(tmp, sst, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, sst);
        }
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        }
        return sst;
    }

    private record IndexEntry(String key, long offset) {
    }
}
