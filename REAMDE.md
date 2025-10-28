# MoniepointKV — a networked, persistent key–value store (Java 21)

A compact take‑home implementation of a Bitcask‑style append‑only KV store with a tiny HTTP API, built on **Java 21** (standard library only). Targets **Amazon Corretto 21**. No third‑party dependencies.

---

## Table of Contents

* [Goals](#goals)
* [Features](#features)
* [Design Overview](#design-overview)
* [On‑disk Format](#on-disk-format)
* [HTTP API](#http-api)
* [Build & Run](#build--run)
* [Quick Start (cURL)](#quick-start-curl)
* [Configuration](#configuration)
* [Persistence & Recovery](#persistence--recovery)
* [Compaction](#compaction)
* [Concurrency Model](#concurrency-model)
* [Performance Notes](#performance-notes)
* [Project Structure](#project-structure)
* [Limitations & Future Work](#limitations--future-work)
* [Why These Choices](#why-these-choices)

---

## Goals

* Deliver a **reliable**, **understandable** KV store suitable for a take‑home assignment.
* Use **only JDK** (no frameworks). Keep the surface area small.
* Optimize for correctness and clarity; leave well‑scoped hooks for growth.

## Features

* **PUT / GET / DELETE**
* **Batch PUT** (contiguous appends; per‑record indexing)
* **Range reads** via lexicographic key order
* **Append‑only segments** with **CRC32**, length framing, and **crash‑safe** tail handling
* **Periodic fsync** (group commit) — configurable interval
* **Index rebuild** on startup (Bitcask‑style)
* **Manual compaction** endpoint to reclaim dead entries
* **Virtual threads** (Java 21) for simple, scalable request handling

> No external dependencies; HTTP served via `jdk.httpserver` from the JDK.

## Design Overview

* **StorageEngine** manages segments and an in‑memory index (`ConcurrentSkipListMap<String, Position>`). Writes are **append‑only**; the index stores file positions for the latest key version.
* **SegmentWriter** serializes records and tracks file size for rotation.
* **Record** defines the wire format with CRC and provides a `ByteBuffer` serializer.
* **Position** is an immutable logical address `(segmentId, offset)`.
* **HTTP**: minimal REST over `HttpServer`, one v‑thread per request.

## On‑disk Format

Each record (big‑endian) is framed as:

```
[ crc32(4) ][ tomb(1) ][ klen(4) ][ vlen(4) ][ key bytes ][ value bytes ]
```

* `crc32` over `(tomb, key, value)`
* `tomb`: 0 = PUT, 1 = DELETE
* `klen`, `vlen`: lengths in bytes
* Keys are stored as UTF‑8 bytes by the engine; values are raw bytes.

During recovery the engine scans sequentially and **stops on the first corrupt/incomplete tail**, avoiding index corruption.

## HTTP API

### `PUT /kv?key=<k>`

Body = raw value bytes. Returns **204**.

### `GET /kv?key=<k>`

Returns **200** with raw bytes (octet‑stream) or **404** if absent.

### `DELETE /kv?key=<k>`

Writes a tombstone and evicts from the index. Returns **204**.

### `POST /batch`

Body: text, **one line per entry** as `key\tvalue`. Returns JSON `{ "written": N }`.

### `GET /range?start=<a>&end=<b>&limit=<n>`

Lexicographic range (inclusive). Returns **ndjson**: one JSON object per line,

```json
{"k":"...","v":"<base64>"}
```

Values are **base64‑encoded** to allow binary payloads. For quick inspection:

```bash
curl -s 'http://localhost:8080/range?start=a&end=z' | jq -r '.k+"\t"+(.v|@base64d)'
```

## Build & Run

### Gradle (Kotlin DSL)

* Java toolchain: 21
* Application plugin with `--add-modules=jdk.httpserver`
* See `build.gradle.kts` in repo. Typical commands:

```bash
./gradlew clean build
./gradlew run --args="--data ./data --port 8080 --segment-bytes 134217728 --fsync-interval-ms 20"
```

### Run the Jar directly

```bash
java --add-modules=jdk.httpserver -jar build/libs/moniepoint-kv-*.jar --data ./data --port 8080
```

### Amazon Corretto 21

Works out of the box (contains `jdk.httpserver`). No extra flags besides `--add-modules=jdk.httpserver` when running the jar.

## Quick Start (cURL)

```bash
# put
curl -X PUT 'http://localhost:8080/kv?key=foo' --data-raw 'bar'

# get
curl -s 'http://localhost:8080/kv?key=foo'

# batch put (key\tvalue per line)
curl -X POST 'http://localhost:8080/batch' --data-binary $'a\tb\nfoo\tBAR2'

# range
docker run --rm -it stedolan/jq jq --version >/dev/null 2>&1 || true
curl -s 'http://localhost:8080/range?start=a&end=g' | jq -r '.k+"\t"+(.v|@base64d)'

# delete
curl -X DELETE 'http://localhost:8080/kv?key=foo'

# compaction
curl -X POST 'http://localhost:8080/compact'
```

## Configuration

* `--data <dir>`: data directory (created if missing)
* `--port <int>`: HTTP port (default: 8080)
* `--segment-bytes <long>`: rotation threshold per segment (default: 128 MiB)
* `--fsync-interval-ms <long>`: periodic fsync; `0` disables scheduler (default: 20 ms)

## Persistence & Recovery

* Append‑only writes + periodic `fsync` → durable with group‑commit latency/throughput balance.
* Startup scans `*.seg` in order and rebuilds the index from last valid record.
* CRC guards against torn tails — scan halts safely at the first invalid header/payload.

## Compaction

* Copies the **live set** (latest version of each key) into a fresh segment.
* Swaps active writer and removes older segment files (best‑effort).
* Trigger: `POST /compact`.

## Concurrency Model

* **Virtual threads** handle requests; blocking I/O is fine and simple.
* `SegmentWriter` serializes writes with synchronized methods (per‑segment ordering guaranteed).
* Readers open their own `FileChannel` instances; no channel sharing across threads.

## Performance Notes

* Sequential appends exploit the OS page cache; fsync batching reduces syscall pressure.
* SkipList index gives O(logN) point lookups and efficient `subMap()` for ranges.
* Batch PUT appends records contiguously and records **per‑key positions** (fixed a bug where a single offset was used for all items in a batch).

## Project Structure

```
src/main/java/kv/
  KV.java     # HTTP server (CLI, endpoints), wiring
  StorageEngine.java    # engine: segments, index, recovery, compaction
  SegmentWriter.java    # append‑only writer for one segment
  Record.java           # record format + serializer
  Position.java         # immutable logical address (segmentId, offset)
```

## Limitations & Future Work

* Index is fully in‑memory (key → position). For very large key sets, add a **sparse on‑disk index** per segment (fence‑pointers + mmap + binary search).
* Background/online compaction with size/garbage ratio policy.
* Simple replication (leader streaming appends to followers), then quorum acks.
* Optional checksumming upgrade (e.g., CRC32C) and record versioning.
* Metrics/observability endpoints.

## Why These Choices

* **Append‑only + rebuild** is a proven design (Bitcask) that is easy to reason about and crash‑friendly.
* **JDK httpserver** keeps the stack dependency‑free and is sufficient for an assignment‑scale service.
* **Virtual threads** make concurrency simple without reactive frameworks.
* **SkipList** gives range scans «for free» with minimal extra complexity.

---

*© 2025 — Take‑home reference implementation for a Principal Engineer assignment.*
