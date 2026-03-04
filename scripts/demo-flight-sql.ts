#!/usr/bin/env bun
/**
 * Demo script showcasing FlightSqlClient usage.
 *
 * Run with: bun run demo:flight-sql
 *
 * Configure the server connection via environment variables:
 * - FLIGHT_HOST: Host address (default: localhost)
 * - FLIGHT_PORT: Port number (default: 50051)
 * - FLIGHT_TLS: Enable TLS (default: false)
 * - FLIGHT_USERNAME: Username for basic auth (optional)
 * - FLIGHT_PASSWORD: Password for basic auth (optional)
 * - FLIGHT_BEARER_TOKEN: Bearer token for auth (optional)
 */

import {
  createFlightSqlClient,
  FlightConnectionError,
  FlightError,
  type FlightSqlClient
} from "../src/client"

// ── Configuration ─────────────────────────────────────────────────────

const host = process.env.FLIGHT_HOST ?? "localhost"
const port = parseInt(process.env.FLIGHT_PORT ?? "50051", 10)
const tls = process.env.FLIGHT_TLS === "true"
const bearerToken = process.env.FLIGHT_BEARER_TOKEN
const username = process.env.FLIGHT_USERNAME
const password = process.env.FLIGHT_PASSWORD

const url = `${tls ? "https" : "http"}://${host}:${String(port)}`

// ── Helpers ───────────────────────────────────────────────────────────

/** JSON replacer that converts BigInt to string. */
function jsonReplacer(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value
}

/** Quote a SQL identifier if it contains special characters. */
function quoteIdentifier(name: string): string {
  // Quote identifiers containing non-alphanumeric characters (except underscore)
  if (/[^a-zA-Z0-9_]/.test(name)) {
    return `"${name.replace(/"/g, '""')}"`
  }
  return name
}

/** Build a fully qualified table name with proper quoting. */
function buildTableName(catalog: string, schema: string, table: string): string {
  return [catalog, schema, table]
    .filter(Boolean)
    .map((part) => quoteIdentifier(part))
    .join(".")
}

function printHeader(title: string): void {
  console.log()
  console.log(`═══ ${title} ═══`)
  console.log()
}

function printSection(title: string): void {
  console.log(`── ${title} ──`)
}

// ── Demo Functions ────────────────────────────────────────────────────

async function demoGetCatalogs(client: FlightSqlClient): Promise<void> {
  printSection("Listing Catalogues")

  try {
    const catalogs = await client.getCatalogs()
    console.log(`  Found ${String(catalogs.numRows)} catalogue(s)`)

    for (let i = 0; i < catalogs.numRows; i++) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const catalogName = catalogs.getChildAt(0)?.get(i)
      console.log(`    - ${String(catalogName)}`)
    }
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
  }
}

async function demoGetDbSchemas(client: FlightSqlClient): Promise<void> {
  printSection("Listing Database Schemas")

  try {
    const schemas = await client.getDbSchemas({})
    console.log(`  Found ${String(schemas.numRows)} schema(s)`)

    const maxDisplay = Math.min(10, schemas.numRows)
    for (let i = 0; i < maxDisplay; i++) {
      /* eslint-disable @typescript-eslint/no-unsafe-assignment */
      const catalogName = schemas.getChildAt(0)?.get(i) ?? "(null)"
      const schemaName = schemas.getChildAt(1)?.get(i)
      /* eslint-enable @typescript-eslint/no-unsafe-assignment */
      console.log(`    - ${String(catalogName)}.${String(schemaName)}`)
    }

    if (schemas.numRows > maxDisplay) {
      console.log(`    ... and ${String(schemas.numRows - maxDisplay)} more`)
    }
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
  }
}

async function demoGetTables(client: FlightSqlClient): Promise<void> {
  printSection("Listing Tables")

  try {
    const tables = await client.getTables({})
    console.log(`  Found ${String(tables.numRows)} table(s)`)

    const maxDisplay = Math.min(10, tables.numRows)
    for (let i = 0; i < maxDisplay; i++) {
      // getTables returns: catalog_name, db_schema_name, table_name, table_type
      /* eslint-disable @typescript-eslint/no-unsafe-assignment */
      const catalogName = tables.getChildAt(0)?.get(i) ?? ""
      const schemaName = tables.getChildAt(1)?.get(i) ?? ""
      const tableName = tables.getChildAt(2)?.get(i)
      const tableType = tables.getChildAt(3)?.get(i)
      /* eslint-enable @typescript-eslint/no-unsafe-assignment */
      console.log(
        `    - ${String(catalogName)}.${String(schemaName)}.${String(tableName)} (${String(tableType)})`
      )
    }

    if (tables.numRows > maxDisplay) {
      console.log(`    ... and ${String(tables.numRows - maxDisplay)} more`)
    }
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
  }
}

async function demoQuery(client: FlightSqlClient): Promise<void> {
  printSection("Executing SQL Query")

  // First, find a table to query
  let tableName = '"test"."integers"' // Default fallback

  try {
    const tables = await client.getTables({})
    if (tables.numRows > 0) {
      // Use the first available table
      const catalog = String(tables.getChildAt(0)?.get(0) ?? "")
      const schema = String(tables.getChildAt(1)?.get(0) ?? "")
      const table = String(tables.getChildAt(2)?.get(0) ?? "")
      tableName = buildTableName(catalog, schema, table)
    }
  } catch {
    // Use default table name
  }

  const sql = `SELECT * FROM ${tableName} LIMIT 5`
  console.log(`  Query: ${sql}`)
  console.log()

  try {
    const table = await client.query(sql)

    console.log(`  Results: ${String(table.numRows)} rows`)
    console.log(`  Schema: ${table.schema.fields.map((f) => f.name).join(", ")}`)
    console.log()

    // Print results as a simple table
    if (table.numRows > 0) {
      console.log("  Data:")
      for (let i = 0; i < table.numRows; i++) {
        const row: Record<string, unknown> = {}
        for (let j = 0; j < table.schema.fields.length; j++) {
          row[table.schema.fields[j].name] = table.getChildAt(j)?.get(i)
        }
        console.log(`    ${JSON.stringify(row, jsonReplacer)}`)
      }
    }
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
  }
}

async function demoQueryBatches(client: FlightSqlClient): Promise<void> {
  printSection("Streaming Query Results (Batches)")

  let tableName = '"test"."integers"'

  try {
    const tables = await client.getTables({})
    if (tables.numRows > 0) {
      const catalog = String(tables.getChildAt(0)?.get(0) ?? "")
      const schema = String(tables.getChildAt(1)?.get(0) ?? "")
      const table = String(tables.getChildAt(2)?.get(0) ?? "")
      tableName = buildTableName(catalog, schema, table)
    }
  } catch {
    // Use default
  }

  const sql = `SELECT * FROM ${tableName} LIMIT 100`
  console.log(`  Query: ${sql}`)
  console.log()

  try {
    let batchCount = 0
    let totalRows = 0

    for await (const batch of client.queryBatches(sql)) {
      batchCount++
      totalRows += batch.numRows
      console.log(`    Batch ${String(batchCount)}: ${String(batch.numRows)} rows`)
    }

    console.log()
    console.log(`  Total: ${String(totalRows)} rows in ${String(batchCount)} batch(es)`)
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
  }
}

async function demoPreparedStatement(client: FlightSqlClient): Promise<void> {
  printSection("Prepared Statement")

  const sql = "SELECT 1 AS value"
  console.log(`  Preparing: ${sql}`)

  try {
    const stmt = await client.prepare(sql)

    console.log(`  Handle: ${String(stmt.handle.length)} bytes`)
    console.log(`  Dataset schema: ${String(stmt.datasetSchema.length)} bytes`)
    console.log(`  Parameter schema: ${String(stmt.parameterSchema.length)} bytes`)

    // Execute the prepared statement
    console.log()
    console.log("  Executing prepared statement...")
    const table = await client.executePrepared(stmt)
    console.log(`  Result: ${String(table.numRows)} row(s)`)

    // Clean up
    console.log()
    console.log("  Closing prepared statement...")
    await client.closePreparedStatement(stmt)
    console.log("  Done")
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
  }
}

async function demoTransaction(client: FlightSqlClient): Promise<void> {
  printSection("Transaction Management")

  try {
    console.log("  Beginning transaction...")
    const txn = await client.beginTransaction()
    console.log(`  Transaction ID: ${String(txn.id.length)} bytes`)

    // Transactions require write operations to demonstrate
    // For this demo, we'll just show the lifecycle
    console.log()
    console.log("  Rolling back transaction...")
    await client.rollback(txn)
    console.log("  Transaction rolled back")
  } catch (error) {
    console.log(`  Error: ${error instanceof Error ? error.message : "unknown error"}`)
    console.log("  (Transaction support varies by server)")
  }
}

// ── Main ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  printHeader("Arrow Flight SQL Client Demo")

  console.log(`Connecting to: ${url}`)

  // Build auth configuration
  let auth
  if (bearerToken !== undefined) {
    auth = { type: "bearer" as const, token: bearerToken }
    console.log("Auth: Bearer token")
  } else if (username !== undefined && password !== undefined) {
    auth = { type: "basic" as const, credentials: { username, password } }
    console.log(`Auth: Basic (user: ${username})`)
  } else {
    console.log("Auth: None")
  }

  // Create the client
  const client = createFlightSqlClient({ url, auth })

  try {
    // Authenticate if using basic auth
    if (auth?.type === "basic") {
      console.log("Authenticating...")
      await client.authenticate()
      console.log("Authentication successful")
    }

    // Run demos
    await demoGetCatalogs(client)
    await demoGetDbSchemas(client)
    await demoGetTables(client)
    await demoQuery(client)
    await demoQueryBatches(client)
    await demoPreparedStatement(client)
    await demoTransaction(client)

    printHeader("Demo Complete")
  } catch (error) {
    console.error()
    if (FlightConnectionError.isError(error)) {
      console.error(`Connection error: ${error.message}`)
      console.error("Ensure the Flight SQL server is running and accessible")
    } else if (FlightError.isError(error)) {
      console.error(`Flight error: ${error.message}`)
    } else {
      console.error("Unexpected error:", error)
    }
    process.exit(1)
  } finally {
    client.close()
  }
}

main().catch(console.error)
