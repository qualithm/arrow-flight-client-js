# Arrow Flight Client

[![CI](https://github.com/qualithm/arrow-flight-client-js/actions/workflows/ci.yaml/badge.svg)](https://github.com/qualithm/arrow-flight-client-js/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/qualithm/arrow-flight-client-js/graph/badge.svg)](https://codecov.io/gh/qualithm/arrow-flight-client-js)
[![npm](https://img.shields.io/npm/v/@qualithm/arrow-flight-client)](https://www.npmjs.com/package/@qualithm/arrow-flight-client)

Unified Arrow Flight and Flight SQL client for JavaScript and TypeScript runtimes. Provides complete
gRPC client support for Apache Arrow Flight and Flight SQL protocols.

## Features

- **Flight RPC** — All core methods: listFlights, getFlightInfo, doGet, doPut, doAction, etc.
- **Flight SQL** — SQL queries, prepared statements, transactions, and database metadata
- **Authentication** — Bearer tokens, mTLS, and Flight Handshake (BasicAuth)
- **Arrow IPC** — Encode/decode Apache Arrow data for streaming transfers
- **Multi-runtime** — Works with Bun, Node.js 20+, and Deno

## Installation

```bash
bun add @qualithm/arrow-flight-client
# or
npm install @qualithm/arrow-flight-client
```

## Quick Start

```ts
import { createFlightClient, decodeFlightDataToTable } from "@qualithm/arrow-flight-client"

const client = createFlightClient({
  url: "http://localhost:50051",
  auth: { type: "bearer", token: "your-token" }
})

// List available flights
for await (const info of client.listFlights()) {
  console.log(info.flightDescriptor?.path)
}

// Fetch data
const flightInfo = await client.getFlightInfo({ type: "path", path: ["my", "dataset"] })
const ticket = flightInfo.endpoint[0].ticket
const table = await decodeFlightDataToTable(client.doGet(ticket))

console.log(`Received ${table.numRows} rows`)

client.close()
```

## Usage

### FlightClient (Core Flight RPC)

```ts
import { createFlightClient, decodeFlightDataToTable } from "@qualithm/arrow-flight-client"

// Create a client
const client = createFlightClient({
  url: "http://localhost:50051",
  auth: { type: "bearer", token: "your-token" }
})

// List available flights
for await (const info of client.listFlights()) {
  console.log(info.flightDescriptor?.path)
}

// Fetch data
const flightInfo = await client.getFlightInfo({ type: "path", path: ["my", "dataset"] })
const ticket = flightInfo.endpoint[0].ticket
const table = await decodeFlightDataToTable(client.doGet(ticket))

console.log(`Received ${table.numRows} rows`)

// Clean up
client.close()
```

### FlightSqlClient (SQL Operations)

```ts
import { createFlightSqlClient } from "@qualithm/arrow-flight-client"

const client = createFlightSqlClient({
  url: "http://localhost:50051",
  auth: { type: "bearer", token: "your-token" }
})

// Execute a query
const table = await client.query("SELECT * FROM users LIMIT 10")
console.log(`Got ${table.numRows} rows`)

// Stream large results
for await (const batch of client.queryBatches("SELECT * FROM large_table")) {
  console.log(`Batch: ${batch.numRows} rows`)
}

// Prepared statements
const stmt = await client.prepare("SELECT * FROM users WHERE id = ?")
const result = await client.executePrepared(stmt)
await client.closePreparedStatement(stmt)

// Transactions
const txn = await client.beginTransaction()
try {
  await client.executeUpdate("INSERT INTO users (name) VALUES ('Alice')", {
    transactionId: txn.id
  })
  await client.commit(txn)
} catch {
  await client.rollback(txn)
}

// Database metadata
const catalogs = await client.getCatalogs()
const schemas = await client.getDbSchemas({})
const tables = await client.getTables({})

client.close()
```

### Authentication

#### Bearer Token

```ts
const client = createFlightClient({
  url: "https://flight-server:50051",
  auth: { type: "bearer", token: "your-api-token" }
})
```

#### Basic Auth (Flight Handshake)

```ts
const client = createFlightClient({
  url: "https://flight-server:50051",
  auth: {
    type: "basic",
    credentials: { username: "user", password: "pass" }
  }
})

// Authenticate (performs Flight Handshake RPC)
await client.authenticate()
```

#### mTLS

```ts
import { readFileSync } from "node:fs"

const client = createFlightClient({
  url: "https://flight-server:50051",
  tls: {
    ca: readFileSync("ca.pem"),
    cert: readFileSync("client.pem"),
    key: readFileSync("client-key.pem")
  }
})
```

### Arrow IPC Utilities

```ts
import {
  encodeTableToFlightData,
  decodeFlightDataToTable,
  decodeFlightDataStream
} from "@qualithm/arrow-flight-client"
import { tableFromArrays } from "apache-arrow"

// Create Arrow data
const table = tableFromArrays({
  id: Int32Array.from([1, 2, 3]),
  name: ["Alice", "Bob", "Charlie"]
})

// Encode for upload
const flightData = encodeTableToFlightData(table)

// Decode a stream
const receivedTable = await decodeFlightDataToTable(flightDataStream)

// Stream batches
for await (const batch of decodeFlightDataStream(flightDataStream)) {
  console.log(`Batch: ${batch.numRows} rows`)
}
```

### Error Handling

```ts
import {
  FlightError,
  FlightConnectionError,
  FlightAuthError,
  FlightServerError,
  FlightTimeoutError
} from "@qualithm/arrow-flight-client"

try {
  await client.query("SELECT * FROM users")
} catch (error) {
  if (FlightConnectionError.isError(error)) {
    console.error("connection failed:", error.message)
  } else if (FlightAuthError.isError(error)) {
    console.error("authentication failed:", error.message)
  } else if (FlightServerError.isError(error)) {
    console.error("server error:", error.message)
  } else if (FlightTimeoutError.isError(error)) {
    console.error("request timed out:", error.message)
  } else if (FlightError.isError(error)) {
    console.error("flight error:", error.message)
  }
}
```

## API Reference

Full API documentation is generated with [TypeDoc](https://typedoc.org/):

```bash
bun run docs
# Output in docs/
```

## Examples

See the [`examples/`](examples/) directory for runnable examples:

| Example                                                 | Description                    |
| ------------------------------------------------------- | ------------------------------ |
| [`flight-client.ts`](examples/flight-client.ts)         | FlightClient operations        |
| [`flight-sql-client.ts`](examples/flight-sql-client.ts) | FlightSqlClient SQL operations |

```bash
bun run examples/flight-client.ts
```

## Development

### Prerequisites

- [Bun](https://bun.sh/) (recommended), Node.js 20+, or [Deno](https://deno.land/)

### Setup

```bash
bun install
```

### Building

```bash
bun run build
```

### Testing

```bash
bun run test              # unit tests
bun run test:integration  # integration tests (requires Flight server)
bun run test:coverage     # with coverage report
```

### Linting & Formatting

```bash
bun run lint
bun run format
bun run typecheck
```

### Benchmarks

```bash
bun run bench      # Server benchmarks (requires Flight server)
bun run bench:ipc  # Arrow IPC benchmarks
```

## Publishing

The package is automatically published to NPM when CI passes on main. Update the version in
`package.json` before merging to trigger a new release.

## Licence

Apache-2.0
