package sdmitry.kv;

import java.io.*;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;

/**
 KV — a network-available persistent Key/Value store using only Java stdlib (Java 21)

 Features
 - PUT/GET/DELETE, Batch PUT, range reads
 - Append-only, Bitcask-style storage with log segments (CRC32 + length framing)
 - Crash-friendly recovery by index rebuild from segments
 - Periodic fsync and group commit (configurable)
 - Concurrent reads/writes using virtual threads
 - Range reads via ConcurrentSkipListMap index
 - Optional compaction endpoint to reclaim stale entries

 cURL quick test
   curl -X PUT 'http://localhost:8080/kv?key=foo' --data-raw 'bar'
   curl -s 'http://localhost:8080/kv?key=foo'
   curl -X POST 'http://localhost:8080/batch' --data-binary $'a\tb\nfoo\tBAR2'
   curl -s 'http://localhost:8080/range?start=a&end=g'
   curl -X DELETE 'http://localhost:8080/kv?key=foo'
   curl -X POST 'http://localhost:8080/compact'

 Notes
 - Keys and values are UTF‑8 bytes; no size limit except segment limits
 - Only JDK classes are used (no third-party libs)
 - This is a compact, production‑style reference; further hardening noted in TODOs
*/
public class KV {

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = parseArgs(args);
        Path dataDir = Paths.get(cli.getOrDefault("--data", "./data"));
        int port = Integer.parseInt(cli.getOrDefault("--port", "8080"));
        long segmentBytes = Long.parseLong(cli.getOrDefault("--segment-bytes", String.valueOf(128L << 20))); // 128MiB
        long fsyncIntervalMs = Long.parseLong(cli.getOrDefault("--fsync-interval-ms", "20"));

        Files.createDirectories(dataDir);

        BitcaskStorageEngine engine = new BitcaskStorageEngine(dataDir, segmentBytes, fsyncIntervalMs);
        engine.start();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 1024);
        ExecutorService vexec = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(vexec);

        server.createContext("/kv", exchange -> {
            try {
                String method = exchange.getRequestMethod();
                Map<String, String> q = splitQuery(exchange.getRequestURI());
                String key = q.get("key");
                if (key == null || key.isEmpty()) {
                    send(exchange, 400, "missing key");
                    return;
                }
                switch (method) {
                    case "PUT" -> {
                        byte[] value = exchange.getRequestBody().readAllBytes();
                        engine.put(key, value);
                        send(exchange, 204, "");
                    }
                    case "GET" -> {
                        byte[] v = engine.read(key);
                        if (v == null) {
                            send(exchange, 404, "");
                        } else {
                            sendBytes(exchange, 200, v);
                        }
                    }
                    case "DELETE" -> {
                        engine.delete(key);
                        send(exchange, 204, "");
                    }
                    default -> send(exchange, 405, "method not allowed");
                }
            } catch (Exception e) {
                send(exchange, 500, "error: " + e.getMessage());
            }
        });

        server.createContext("/batch", exchange -> {
            try {
                if (!exchange.getRequestMethod().equals("POST")) {
                    send(exchange, 405, "");
                    return;
                }
                // text/plain with lines: key\tvalue
                byte[] body = exchange.getRequestBody().readAllBytes();
                int count = engine.batchPut(parseBatch(body));
                send(exchange, 200, "{\"written\": " + count + "}");
            } catch (Exception e) {
                send(exchange, 500, e.toString());
            }
        });

        server.createContext("/range", exchange -> {
            try {
                if (!exchange.getRequestMethod().equals("GET")) {
                    send(exchange, 405, "");
                    return;
                }
                Map<String, String> q = splitQuery(exchange.getRequestURI());
                String start = q.getOrDefault("start", "");
                String end = q.getOrDefault("end", "\uffff");
                int limit = Integer.parseInt(q.getOrDefault("limit", "1000"));
                List<Map.Entry<String, byte[]>> items = engine.readKeyRange(start, end, limit);
                // JSON lines: {"k":"...","v":"base64..."}\n
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                for (var e : items) {
                    out.write('{');
                    out.write(quote("k").getBytes());
                    out.write(':');
                    out.write(quote(e.getKey()).getBytes());
                    out.write(',');
                    out.write(quote("v").getBytes());
                    out.write(':');
                    out.write(quote(Base64.getEncoder().encodeToString(e.getValue())).getBytes());
                    out.write('}');
                    out.write('\n');
                }
                sendBytes(exchange, 200, out.toByteArray());
            } catch (Exception e) {
                send(exchange, 500, e.toString());
            }
        });

        server.createContext("/compact", exchange -> {
            try {
                if (!exchange.getRequestMethod().equals("POST")) {
                    send(exchange, 405, "");
                    return;
                }
                long reclaimed = engine.compact();
                send(exchange, 200, "{\"reclaimedBytes\": " + reclaimed + "}");
            } catch (Exception e) {
                send(exchange, 500, e.toString());
            }
        });

        server.start();
        System.out.println("KV listening on http://localhost:" + port);
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            m.put(args[i], i + 1 < args.length ? args[i + 1] : "");
        }
        return m;
    }

    static Map<String, String> splitQuery(URI u) {
        Map<String, String> q = new HashMap<>();
        String raw = u.getRawQuery();
        if (raw == null) return q;
        for (String p : raw.split("&")) {
            int idx = p.indexOf('=');
            if (idx < 0) q.put(urlDecode(p), "");
            else q.put(urlDecode(p.substring(0, idx)), urlDecode(p.substring(idx + 1)));
        }
        return q;
    }

    static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    static String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    static List<Map.Entry<String, byte[]>> parseBatch(byte[] body) {
        List<Map.Entry<String, byte[]>> xs = new ArrayList<>();
        String all = new String(body, StandardCharsets.UTF_8);
        for (String line : all.split("\n")) {
            if (line.isEmpty()) continue;
            int t = line.indexOf('\t');
            if (t < 0) continue;
            String k = line.substring(0, t);
            byte[] v = line.substring(t + 1).getBytes(StandardCharsets.UTF_8);
            xs.add(Map.entry(k, v));
        }
        return xs;
    }

    static void send(HttpExchange ex, int code, String text) throws IOException {
        byte[] b = text.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    static void sendBytes(HttpExchange ex, int code, byte[] b) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    static int crc32(byte tomb, byte[] key, byte[] value) {
        CRC32 crc = new CRC32();
        crc.update(tomb);
        crc.update(key);
        crc.update(value);
        return (int) crc.getValue();
    }
}
