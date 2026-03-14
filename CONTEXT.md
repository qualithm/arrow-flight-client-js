# CONTEXT.md

> **This is the single source of truth for this repository.** When CONTEXT.md conflicts with any
> other document, CONTEXT.md is correct.

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

### Features

| Feature            | Status      | Notes                                                         |
| ------------------ | ----------- | ------------------------------------------------------------- |
| Flight RPC methods | Implemented | FlightClient base + doGet/doPut (133 tests)                   |
| Flight SQL         | Implemented | Query, prepared statements, transactions, metadata (31 tests) |
| Arrow IPC          | Implemented | Encode/decode via apache-arrow; tableFromArrays, RecordBatch  |
| Authentication     | Implemented | Bearer tokens, mTLS (TLS options), Handshake (BasicAuth)      |
| Integration tests  | Implemented | Env-based config; tests for Flight RPC and Flight SQL         |
| Cross-runtime      | Validated   | Bun, Node.js 22+, Deno (with import map)                      |

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

### Core Protocol

- [x] Vendor Flight.proto and FlightSql.proto from apache/arrow
- [x] Set up buf code generation with ConnectRPC
- [x] Implement FlightClient base class with connection management
- [x] Implement authentication (Bearer tokens, mTLS, Handshake)
- [x] Implement Arrow IPC stream encoding/decoding

Acceptance: All core Flight RPC methods operational against a test server

### Flight SQL

- [x] Implement SQL query execution
- [x] Implement prepared statement management
- [x] Implement transaction support
- [x] Implement database metadata queries

Acceptance: All Flight SQL commands and actions functional

### Integration Testing

- [x] Create integration test infrastructure with environment-based config
- [x] FlightClient tests: connection, handshake, listFlights, getFlightInfo, getSchema, doGet,
      doPut, doAction, listActions
- [x] FlightSqlClient tests: queries, updates, prepared statements, transactions, metadata

Acceptance: Integration tests runnable via `bun test:integration` against any compliant Flight SQL
server

### Benchmarks

- [x] Create benchmark infrastructure with warmup and statistics
- [x] Benchmark Arrow IPC encoding/decoding (read + write)
- [x] Benchmark column type performance (Int32, Float64, String, Mixed)
- [x] Benchmark round-trip operations (encode + decode)
- [x] Benchmark FlightClient reads (listFlights, getFlightInfo, doGet)
- [x] Benchmark FlightClient writes (doPut)
- [x] Benchmark FlightSqlClient reads (query, prepared statements, metadata)
- [x] Benchmark FlightSqlClient writes (executeUpdate INSERT/UPDATE/DELETE)

Acceptance: `bun run bench` produces server metrics (reads + writes); `bun run bench:ipc` produces
IPC metrics

### Documentation

- [x] Generate TypeDoc API documentation
- [x] Create usage examples for FlightClient
- [x] Create usage examples for FlightSqlClient
- [x] Update README with comprehensive usage guide

Acceptance: Complete API docs and runnable examples for all major features

### Cross-Runtime & Quality

- [x] Cross-runtime validation script (Bun, Node.js, Deno)
- [x] Remove legacy greet.ts module
- [x] Update examples to use package imports
- [x] Bundle size analysis script
- [x] Server compatibility documentation
- [x] Deno import map configuration (deno.json)

Acceptance: `bun run validate:runtime` passes for all runtimes; `bun run analyze:bundle` shows
tree-shaking impact
