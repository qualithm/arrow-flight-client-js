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

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                                                                                                                                                       |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2026-03-02 | Vendored protos at commit aae49e8ba2; FlightSql.proto imports Flight.proto via package path                                                                                                                                                                                    |
| 2026-03-02 | Generated proto code excluded from eslint and tsconfig.node.json (uses enums, @ts-nocheck)                                                                                                                                                                                     |
| 2026-03-02 | ConnectRPC v1.7.0 required; codegen and runtime versions must match (protoc-gen-connect-es)                                                                                                                                                                                    |
| 2026-03-02 | Removed erasableSyntaxOnly from tsconfig.node.json; generated code uses enums incompatible with it                                                                                                                                                                             |
| 2026-03-02 | FlightClient tests use vi.mock for ConnectRPC and generated proto modules to avoid network calls                                                                                                                                                                               |
| 2026-03-03 | Authentication uses three patterns: Bearer (via headers), mTLS (via nodeOptions), Handshake (BasicAuth proto via bidirectional streaming)                                                                                                                                      |
| 2026-03-03 | gRPC transport doesn't need httpVersion parameter; createGrpcTransport always uses HTTP/2                                                                                                                                                                                      |
| 2026-03-03 | Arrow IPC via apache-arrow v21: use tableFromArrays for test data; RecordBatchReader.from returns sync reader for Uint8Array input                                                                                                                                             |
| 2026-03-03 | FlightSqlClient uses composition over inheritance; wraps FlightClient and delegates core RPC operations                                                                                                                                                                        |
| 2026-03-03 | Integration tests use FLIGHT_HOST, FLIGHT_PORT, FLIGHT_TLS env vars for config; mirrored pattern from arrow-flight-js and arrow-flight-sql-js                                                                                                                                  |
| 2026-03-03 | **Flight SQL commands require protobuf Any wrapper** — Commands must be serialized as `google.protobuf.Any` with `type_url: "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"` (etc.) and `value: <proto_bytes>`. Raw proto bytes are rejected by servers. |
| 2026-03-03 | Some Flight SQL servers use REST API for auth tokens instead of Flight Handshake RPC. Pass token via `auth: { type: "bearer", token }` and `FLIGHT_BEARER_TOKEN` env var.                                                                                                      |
| 2026-03-04 | Demo scripts use env vars (FLIGHT_HOST, FLIGHT_PORT, FLIGHT_TLS, FLIGHT_USERNAME, FLIGHT_PASSWORD, FLIGHT_BEARER_TOKEN) for configuration. Run via `bun run demo:flight` and `bun run demo:flight-sql`.                                                                        |
| 2026-03-04 | Benchmarks cover Arrow IPC encode/decode performance with percentiles (P50, P95, P99), throughput (rows/s, MB/s), and coefficient of variation. Configure via WARMUP_ITERATIONS, BENCH_ITERATIONS, BENCH_SIZES env vars. Run via `bun run bench:ipc`.                          |
| 2026-03-05 | Server benchmarks (`bun run bench`) test both reads and writes: FlightClient (listFlights, getFlightInfo, doGet, doPut) and FlightSqlClient (query, prepared statements, metadata, executeUpdate). Uses BENCH_WRITES=false to skip writes, BENCH_WRITE_ROWS for row count.     |
| 2026-03-05 | TypeDoc generates API docs to `docs/` directory. Examples in `examples/` are excluded from strict lint rules (no-console, strict-boolean-expressions) via eslint.base.config.ts scripts-overrides section. Run `bun run docs` to regenerate.                                   |
| 2026-03-05 | **Deno requires import maps for npm packages.** Bare specifiers like `@connectrpc/connect-node` don't resolve in Deno. Use `deno.json` with `imports` mapping to `npm:` specifiers. Subpaths need explicit mappings (e.g., `@bufbuild/protobuf/wkt`).                          |
| 2026-03-05 | Cross-runtime validation script (`bun run validate:runtime`) tests imports on Bun, Node.js, and Deno. Creates temp directory, generates test file, runs each runtime. Validates all exports and client instantiation.                                                          |
| 2026-03-05 | Bundle analysis (`bun run analyze:bundle`): Full library ~187KB raw / 37KB gzip. FlightClient-only ~46KB raw / 12KB gzip due to tree-shaking. FlightSql_pb.js is largest file at 114KB due to extensive SQL commands.                                                          |
| 2026-03-05 | Removed demo scripts (scripts/demo-\*.ts) in favour of examples/ folder. Examples use package imports (`@qualithm/arrow-flight-client`) and are runnable directly with `bun run examples/flight-client.ts`.                                                                    |
