# CONTEXT.md

> **Single source of truth.** CONTEXT.md > Code > README > Comments.

---

## System Intent

Unified Arrow Flight and Flight SQL client for JavaScript and TypeScript runtimes. Provides a
complete gRPC client for Apache Arrow Flight and Flight SQL protocols.

**Key capabilities:**

- Flight RPC (all core methods)
- Flight SQL (queries, prepared statements, transactions, metadata)
- Authentication (Bearer, mTLS, Handshake)
- Arrow IPC encoding/decoding
- Bun, Node.js, and Deno runtime support

**Scope:** Client-only; excludes server implementation, SQL parsing, ORM, and connection pooling.

---

## Current Reality

### Architecture

| Component | Technology             |
| --------- | ---------------------- |
| Language  | TypeScript (ESM-only)  |
| Runtime   | Bun, Node.js 20+, Deno |
| Build     | TypeScript compiler    |
| Test      | Vitest                 |
| Lint      | ESLint, Prettier       |
| Docs      | TypeDoc                |

### Modules

| Module     | Purpose                                                          |
| ---------- | ---------------------------------------------------------------- |
| `index.ts` | Main entry point                                                 |
| `client/`  | FlightClient, FlightSqlClient, errors, types, factory, IPC utils |
| `testing/` | Test helpers, builders, descriptors, streams                     |

### Features

| Feature            | Notes                                                        |
| ------------------ | ------------------------------------------------------------ |
| Flight RPC methods | FlightClient base + doGet/doPut                              |
| Flight SQL         | Query, prepared statements, transactions, metadata           |
| Arrow IPC          | Encode/decode via apache-arrow; tableFromArrays, RecordBatch |
| Authentication     | Bearer tokens, mTLS, Handshake                               |
| Integration tests  | Flight RPC and Flight SQL                                    |
| Cross-runtime      | Bun, Node.js, Deno                                           |

### File Structure

| Directory     | Purpose                                  |
| ------------- | ---------------------------------------- |
| `bench/`      | Benchmarks with stats                    |
| `docs/`       | Generated TypeDoc + server compatibility |
| `examples/`   | Runnable usage examples                  |
| `proto/`      | Vendored proto files from apache/arrow   |
| `scripts/`    | Development utilities                    |
| `src/`        | Source code                              |
| `src/client/` | FlightClient implementation              |
| `src/gen/`    | Generated proto TypeScript (buf)         |

---

## Locked Decisions

1. **Unified library** — Single package providing both Flight and Flight SQL functionality
2. **Client-only** — No server implementation; focus on client consumption
3. **Apache Arrow JS integration** — Use official `apache-arrow` package for IPC handling
4. **Authentication: All patterns** — Bearer tokens, mTLS, and Flight Handshake for maximum
   compatibility
5. **Vendored protos** — Vendor Flight.proto and FlightSql.proto from apache/arrow; compile with buf
   for reproducible builds
6. **gRPC: ConnectRPC** — Cross-runtime support (Node, Bun, Deno); smaller bundle than
   `@grpc/grpc-js`
7. **Explicit resource lifecycle** — User controls prepared statement and transaction lifecycle; no
   implicit caching; explicit `beginTransaction`/`commit`/`rollback`
8. **Minimal runtime deps** — Bundle size, supply chain risk
9. **Factory functions** — Provide `createFlightClient()` and `createFlightSqlClient()` alongside
   class constructors
10. **Static error helpers** — Error classes include static `isError()` methods for type narrowing
11. **Composition for FlightSqlClient** — FlightSqlClient wraps FlightClient via composition (not
    inheritance) for flexibility

---

## Open Decisions & Risks

### Open Decisions

| ID  | Question | Context |
| --- | -------- | ------- |

### Risks

| ID  | Risk                            | Impact | Mitigation                                                                   |
| --- | ------------------------------- | ------ | ---------------------------------------------------------------------------- |
| R-1 | Large bundle size               | Medium | Tree-shaking supported; `bun run analyze:bundle` shows 46KB for FlightClient |
| R-2 | Server compatibility variations | Medium | Server compatibility docs; env-configurable integration tests                |

---

## Work In Flight

> Claim work before starting. Include start timestamp. Remove within 24 hours of completion.

| ID  | Agent | Started | Task | Files |
| --- | ----- | ------- | ---- | ----- |

---

## Work Queue
