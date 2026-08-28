# Mini Redis

A from-scratch, dependency-free reimplementation of core Redis functionality in Java 21, built to deeply understand the mechanics behind an in-memory data store: single-threaded NIO event loops, wire protocol parsing, LRU eviction, TTL expiry, and append-only-file persistence.

Point real `redis-cli` at it on port 6379 and it behaves like Redis — because it speaks RESP, the actual Redis wire protocol, not a custom approximation.

---

## Why this exists

Most "build your own Redis" tutorials stop at a `HashMap` behind a socket. That's not the hard part. The hard part — and the actual point of this project — is:

- A **single-threaded reactor** (one thread, `java.nio.Selector`, non-blocking sockets) serving many concurrent clients with zero locks, because there's never more than one thread touching shared state.
- A **RESP decoder that survives real TCP behavior** — partial frames (a command split across multiple socket reads) and pipelining (multiple commands arriving in one read) — proven with tests that feed the decoder one byte at a time.
- **LRU eviction that's actually LRU**, not FIFO-in-disguise — verified live by reading a key to refresh its recency, then proving it survives eviction while an untouched key doesn't.
- **AOF persistence** that reconstructs exact prior state after a real process kill, by replaying logged commands through the same decode → dispatch pipeline live clients use.

Every one of these was built, tested, and then proven against real `redis-cli` — not just internal test assertions.

---

## Feature list

| Category | Commands / Behavior |
|---|---|
| Strings | `SET` (with `EX seconds`), `GET`, `DEL`, `EXISTS`, `INCR` |
| Expiry | `EXPIRE`, `TTL`, passive expiry on read, active background sampling |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE` (negative-index support) |
| Eviction | LRU, capacity-triggered, via `LinkedHashMap` access-order mode |
| Persistence | AOF — every mutation logged in RESP format, replayed on startup |
| Protocol | Full RESP encode/decode, partial-frame and pipelining safe |
| Compatibility | Verified against real `redis-cli`, not a custom client |

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │           ReactorServer                    │
                    │        (single-threaded reactor)           │
                    └─────────────────────────────────────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    │   Selector (java.nio.channels)   │
                    │   - 1 ServerSocketChannel        │
                    │   - N SocketChannels (clients)   │
                    └───────────────┬────────────────┘
                                    │ selector.select(timeout)
        ┌───────────────────────────┼───────────────────────────┐
        │                            │                            │
   OP_ACCEPT                    OP_READ                     OP_WRITE
        │                            │                            │
        ▼                            ▼                            ▼
  accept new client        read bytes -> per-client         flush queued
  register OP_READ         RespDecoder.feed()                response bytes
                                    │
                                    ▼
                          RespDecoder.tryDecode()
                       (handles partial frames + pipelining)
                                    │
                                    ▼
                          CommandDispatcher.dispatch()
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
              StringCommands   ListCommands    KeyValueStore
                                                (LRU + TTL)
                                    │
                          if mutating & successful
                                    ▼
                              AofWriter.append()
                          (fsync'd RESP-encoded log)
```

**Startup sequence:** `AofReplayer` reads and replays the log **before** `ReactorServer.start()` is ever called — this ordering is structural, not incidental, so no client can ever observe partially-reconstructed state.

### Module breakdown

```
src/main/java/com/miniredis/
├── Main.java                 entrypoint: wires dependencies, replays AOF, starts reactor
├── server/
│   ├── ReactorServer.java    the NIO event loop
│   └── ClientConnection.java per-client decoder + write queue
├── protocol/
│   ├── RespValue.java        sealed type modeling all 5 RESP wire types
│   ├── RespDecoder.java      bytes -> RespValue (stateful, partial-frame safe)
│   └── RespEncoder.java      RespValue -> bytes
├── command/
│   ├── Command.java          functional interface: execute(args, store) -> RespValue
│   ├── CommandDispatcher.java name -> Command routing
│   ├── string/                SET, GET, DEL, EXISTS, INCR, EXPIRE, TTL
│   └── list/                  LPUSH, RPUSH, LPOP, RPOP, LRANGE
├── store/
│   ├── StoredValue.java      sealed type: StringValue | ListValue
│   ├── KeyValueStore.java    LinkedHashMap-backed LRU + TTL map + passive expiry
│   └── ExpiryEngine.java     cooperative active-expiry sampler
└── persistence/
    ├── AofWriter.java        appends mutating commands, RESP-encoded, fsync'd
    └── AofReplayer.java      replays the log on startup
```

There's no traditional MVC split here — this isn't a web app. The layering that actually matters is **protocol** (bytes) → **command** (semantics) → **store** (state), each with zero knowledge of the layers around it. `CommandDispatcher`, for example, has no idea RESP encoding exists; it only knows plain strings in, `RespValue` out.

---

## Design decisions worth defending in an interview

**Why single-threaded instead of thread-per-connection or a thread pool?**
Thread-per-connection means one OS thread blocked per client, mostly idle, costing stack memory and scheduler overhead at scale. The reactor pattern uses one thread and a `Selector` (OS-level `epoll`-equivalent) to ask "which of these sockets actually has data right now?" instead of blocking on each individually. This is *why* the RESP decoder had to be built stateful and partial-frame-safe: a non-blocking `read()` can return zero bytes or a fragment, and the thread must never wait around for more — it has to immediately go check other sockets.

**Why no synchronization anywhere in `KeyValueStore`?**
Because there's structurally only ever one thread touching it — the reactor thread. Adding locks would be dead weight for a guarantee the architecture already provides. If this store were ever used from multiple threads, that assumption would need explicit revisiting; it's a documented constraint, not an oversight.

**Why `LinkedHashMap` in access-order mode for LRU, instead of hand-rolling a doubly-linked list + hashmap?**
The JDK already provides a correct, O(1) implementation via the access-order constructor flag plus `removeEldestEntry()`. Hand-rolling the same thing would be re-implementing a solved problem for no behavioral gain — recognizing when *not* to build something is as much a design decision as building it.

**Why no `EvictionPolicy` interface abstraction?**
YAGNI. `LinkedHashMap` already provides one correct, tested eviction strategy. Introducing an interface to hypothetically support a second algorithm (e.g. LFU) that isn't an actual requirement would be speculative abstraction — indirection with no current payoff. If a second strategy became a real need, that's when the interface earns its place.

**Why AOF fsyncs after every write instead of buffering?**
Real Redis makes this configurable (`always` / `everysec` / `no`) as an explicit durability-vs-throughput trade-off. This project defaults to `always` — correctness-by-default, at the cost of a real disk I/O per mutating command. Under heavy write load this meaningfully limits throughput; a known, stated limitation rather than an oversight.

**Why does `AofReplayer` skip malformed log entries instead of aborting replay entirely?**
A crash mid-write could leave a truncated final entry. Aborting the whole replay over one bad trailing entry would discard every valid command that came before it too — strictly worse for data recovery than skipping just the broken entry.

**Why does `Command.isMutating()` default to `true`?**
Fail-safe direction matters. If a new command is added and someone forgets this method exists, the safe failure is "logged a read command unnecessarily" (harmless), not "silently failed to log a write command" (real data loss). Read-only commands opt out explicitly.

---

## Setup

**Requirements:**
- JDK 21+ (developed against JDK 22; targets `--release 21` for the sealed-interface pattern-matching-in-switch syntax)
- Maven 3.9+
- No external runtime dependencies — JUnit 5 only, test-scoped

**Build:**
```bash
mvn compile
```

**Test:**
```bash
mvn test
```

**Run:**
```bash
java -cp target/classes com.miniredis.Main
```

Starts listening on port `6379`. On startup it replays `mini-redis.aof` (created in the working directory) if one exists, then begins accepting connections.

**Connect with real Redis tooling:**
```bash
redis-cli -p 6379
```
(On Windows, native `redis-cli` isn't available — see the WSL notes below.)

### Configuration

No environment variables or config file yet — two values are currently hardcoded, both easy to relocate to config later:

| Setting | Location | Value |
|---|---|---|
| Port | `ReactorServer.PORT` | `6379` |
| Max keys before LRU eviction | `Main.MAX_KEYS` | `10_000` |
| Active expiry cycle interval | `ReactorServer.EXPIRY_CYCLE_INTERVAL_MS` | `100` ms |
| AOF file path | `Main.main()` | `mini-redis.aof` (relative to working directory) |

### Windows / WSL networking note

WSL2 cannot always reach a Windows-hosted server via plain `localhost`. If `redis-cli -p 6379` from WSL returns "Connection refused":
1. Ensure a Windows Firewall inbound rule allows TCP port 6379.
2. From WSL, run `ip route | grep default` to get the Windows host IP, then connect with `redis-cli -h <that-ip> -p 6379`.

---

## Command reference

| Command | Syntax | Notes |
|---|---|---|
| `SET` | `SET key value [EX seconds]` | Clears any existing TTL unless `EX` given |
| `GET` | `GET key` | Returns `(nil)` on missing key |
| `DEL` | `DEL key [key2 ...]` | Returns count of keys actually deleted |
| `EXISTS` | `EXISTS key [key2 ...]` | Returns count, not boolean — counts duplicates |
| `INCR` | `INCR key` | Treats missing key as 0; errors on non-integer or overflow |
| `EXPIRE` | `EXPIRE key seconds` | Returns 0 if key didn't exist |
| `TTL` | `TTL key` | `-2` missing, `-1` no TTL, `>=0` seconds remaining |
| `LPUSH` | `LPUSH key val [val2 ...]` | Each value pushed to head in turn (order reverses) |
| `RPUSH` | `RPUSH key val [val2 ...]` | Each value appended to tail (order preserved) |
| `LPOP` | `LPOP key` | Deletes the key if the list becomes empty |
| `RPOP` | `RPOP key` | Same as above, from the tail |
| `LRANGE` | `LRANGE key start stop` | Inclusive both ends; negative indices count from tail |

All error messages and edge-case sentinels (`(nil)`, `-2`/`-1` TTL convention, `WRONGTYPE` errors) match real Redis's actual wire behavior, not an approximation.

---

## Testing

41 automated tests across every layer, plus manual verification against real `redis-cli` for every feature:

```
src/test/java/com/miniredis/
├── protocol/RespDecoderTest.java              8 tests — partial frames, pipelining, malformed input
├── command/string/SetGetCommandTest.java       8 tests
├── command/string/DelExistsIncrCommandTest.java 12 tests
├── command/EndToEndPipelineTest.java           8 tests — bytes to response, no socket
└── persistence/AofPersistenceTest.java         6 tests — write/replay round trip, ordering
```

Notably, `RespDecoderTest` includes a test that feeds a complete command **one byte at a time**, simulating worst-case TCP fragmentation — this is the test most from-scratch Redis clones skip, and it's the one most likely to catch a real off-by-one in frame boundary handling.

---
## Testing 1,000 and 10,000 Concurrent Connections

The Mini-Redis server was load-tested with **1,000**, **1,024**, and **10,000 simultaneous client connections**.

> **Note:** These tests measure connection handling capability and connection throughput. They do not represent sustained Redis command throughput.

### Test Environment

* **Client:** Python load test script
* **Server:** Mini-Redis running on WSL
* **Host:** `172.20.112.1`
* **Port:** `6379`

---

### 1,000 Concurrent Connections

```powershell
python load_test.py --connections 1000 --host 172.20.112.1 --port 6379
```

**Result:**

```text
Total connections attempted : 1000
Succeeded                   : 1000
Failed                      : 0
Total wall time             : 2.13 seconds
Connections per second      : 469.8
```

| Metric                |   Result |
| --------------------- | -------: |
| Connections attempted |    1,000 |
| Successful            |    1,000 |
| Failed                |        0 |
| Total wall time       |   2.13 s |
| Connections/sec       |    469.8 |
| Success rate          | **100%** |

---

### 1,024 Concurrent Connections

```powershell
python load_test.py --connections 1024 --host 172.20.112.1 --port 6379
```

**Result:**

```text
Total connections attempted : 1024
Succeeded                   : 1024
Failed                      : 0
Total wall time             : 1.72 seconds
Connections per second      : 594.9
```

| Metric                |   Result |
| --------------------- | -------: |
| Connections attempted |    1,024 |
| Successful            |    1,024 |
| Failed                |        0 |
| Total wall time       |   1.72 s |
| Connections/sec       |    594.9 |
| Success rate          | **100%** |

---

### 10,000 Concurrent Connections

```powershell
python load_test.py --connections 10000 --host 172.20.112.1 --port 6379
```

**Result:**

```text
Total connections attempted : 10000
Succeeded                   : 10000
Failed                      : 0
Total wall time             : 17.25 seconds
Connections per second      : 579.8
```

| Metric                |   Result |
| --------------------- | -------: |
| Connections attempted |   10,000 |
| Successful            |   10,000 |
| Failed                |        0 |
| Total wall time       |  17.25 s |
| Connections/sec       |    579.8 |
| Success rate          | **100%** |

---

### Summary

| Concurrent Connections | Successful | Failed | Wall Time | Connections/sec |
| ---------------------: | ---------: | -----: | --------: | --------------: |
|                  1,000 |      1,000 |      0 |    2.13 s |           469.8 |
|                  1,024 |      1,024 |      0 |    1.72 s |           594.9 |
|                 10,000 |     10,000 |      0 |   17.25 s |           579.8 |

The server successfully accepted **all 10,000 concurrent connection attempts with zero failures**, demonstrating that the Java NIO reactor architecture can handle a high number of simultaneous client connections in the tested environment.


## Known limitations (stated, not hidden)

- **AOF has no compaction/rewrite.** The log grows forever; real Redis periodically rewrites it to a compact snapshot. Not implemented here — a natural next step.
- **`AofWriter` reopens the file on every append** to call `force()`. Correct, but reopens a `FileChannel` per write rather than holding one open for the writer's lifetime — a known, explainable performance trade-off favoring simplicity.
- **No authentication, no replication, no clustering** — out of scope for this project's learning goals.
- **Bulk strings are treated as UTF-8 text**, not fully binary-safe, though the decoder's byte-level parsing would support binary values with minor changes to how results are exposed.

---

## What I'd change with more time

- Config file / env vars instead of hardcoded constants
- AOF rewrite/compaction
- A long-lived `FileChannel` in `AofWriter` instead of reopen-per-write
- `PERSIST`, `LLEN`, `TYPE` commands to round out compatibility further
