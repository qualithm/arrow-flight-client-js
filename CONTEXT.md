# CONTEXT.md

> **This is the single source of truth for this repository.** When CONTEXT.md conflicts with any
> other document, CONTEXT.md is correct.

---

## System Intent

Unified Arrow Flight client for JavaScript and TypeScript runtimes. Provides a complete gRPC client
for Apache Arrow Flight and Flight SQL protocols.

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

| Module     | Purpose                                         |
| ---------- | ----------------------------------------------- |
| `index.ts` | Main entry point                                |
| `client/`  | FlightClient, errors, types, factory, IPC utils |
| `greet.ts` | Greeting utility (legacy, to be removed)        |

### File Structure

| Directory     | Purpose                                |
| ------------- | -------------------------------------- |
| `bench/`      | Benchmarks with stats                  |
| `examples/`   | Runnable usage examples                |
| `proto/`      | Vendored proto files from apache/arrow |
| `scripts/`    | Development utilities                  |
| `src/`        | Source code                            |
| `src/client/` | FlightClient implementation            |
| `src/gen/`    | Generated proto TypeScript (buf)       |

### Features

| Feature            | Status      | Notes                                                        |
| ------------------ | ----------- | ------------------------------------------------------------ |
| Flight RPC methods | In progress | FlightClient base + doGet/doPut (133 tests)                  |
| Flight SQL         | Not started | —                                                            |
| Arrow IPC          | Implemented | Encode/decode via apache-arrow; tableFromArrays, RecordBatch |
| Authentication     | Implemented | Bearer tokens, mTLS (TLS options), Handshake (BasicAuth)     |

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

---

## Open Decisions & Risks

### Open Decisions

| ID   | Question                                        | Context                                                              |
| ---- | ----------------------------------------------- | -------------------------------------------------------------------- |
| OD-1 | Inheritance or composition for FlightSqlClient? | Inheritance simpler; composition more flexible for future extensions |

### Risks

| ID  | Risk                            | Impact | Mitigation                               |
| --- | ------------------------------- | ------ | ---------------------------------------- |
| R-1 | Large bundle size               | Medium | Tree-shaking, optional heavy deps        |
| R-2 | Server compatibility variations | Medium | Test against multiple Flight SQL servers |

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

- [ ] Implement SQL query execution
- [ ] Implement prepared statement management
- [ ] Implement transaction support
- [ ] Implement database metadata queries

Acceptance: All Flight SQL commands and actions functional

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                  |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 2026-03-02 | Vendored protos at commit aae49e8ba2; FlightSql.proto imports Flight.proto via package path                                               |
| 2026-03-02 | Generated proto code excluded from eslint and tsconfig.node.json (uses enums, @ts-nocheck)                                                |
| 2026-03-02 | ConnectRPC v1.7.0 required; codegen and runtime versions must match (protoc-gen-connect-es)                                               |
| 2026-03-02 | Removed erasableSyntaxOnly from tsconfig.node.json; generated code uses enums incompatible with it                                        |
| 2026-03-02 | FlightClient tests use vi.mock for ConnectRPC and generated proto modules to avoid network calls                                          |
| 2026-03-03 | Authentication uses three patterns: Bearer (via headers), mTLS (via nodeOptions), Handshake (BasicAuth proto via bidirectional streaming) |
| 2026-03-03 | gRPC transport doesn't need httpVersion parameter; createGrpcTransport always uses HTTP/2                                                 |
| 2026-03-03 | Arrow IPC via apache-arrow v21: use tableFromArrays for test data; RecordBatchReader.from returns sync reader for Uint8Array input        |
