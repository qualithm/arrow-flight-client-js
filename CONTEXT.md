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

| Module     | Purpose          |
| ---------- | ---------------- |
| `index.ts` | Main entry point |
| `greet.ts` | Greeting utility |

### File Structure

| Directory   | Purpose                 |
| ----------- | ----------------------- |
| `bench/`    | Benchmarks with stats   |
| `examples/` | Runnable usage examples |
| `scripts/`  | Development utilities   |
| `src/`      | Source code             |

### Features

| Feature            | Status      | Notes |
| ------------------ | ----------- | ----- |
| Flight RPC methods | Not started | —     |
| Flight SQL         | Not started | —     |
| Arrow IPC          | Not started | —     |
| Authentication     | Not started | —     |

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

- [ ] Vendor Flight.proto and FlightSql.proto from apache/arrow
- [ ] Set up buf code generation with ConnectRPC
- [ ] Implement FlightClient base class with connection management
- [ ] Implement authentication (Bearer tokens, mTLS, Handshake)
- [ ] Implement Arrow IPC stream encoding/decoding

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

| Date | Learning |
| ---- | -------- |
