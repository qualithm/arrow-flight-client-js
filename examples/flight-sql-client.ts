/* eslint-disable no-console */
/**
 * FlightSqlClient usage example.
 *
 * This example demonstrates Arrow Flight SQL operations:
 * - Creating a client with authentication
 * - Executing SQL queries
 * - Streaming large result sets
 * - Using prepared statements
 * - Managing transactions
 * - Querying database metadata
 *
 * Run with: bun run examples/flight-sql-client.ts
 *
 * Configure the server connection via environment variables:
 * - FLIGHT_HOST: Host address (default: localhost)
 * - FLIGHT_PORT: Port number (default: 50051)
 * - FLIGHT_TLS: Enable TLS (default: false)
 * - FLIGHT_BEARER_TOKEN: Bearer token for auth (optional)
 */

import {
  createFlightSqlClient,
  FlightConnectionError,
  FlightError
} from "@qualithm/arrow-flight-client"

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const host = process.env.FLIGHT_HOST ?? "localhost"
const port = parseInt(process.env.FLIGHT_PORT ?? "50051", 10)
const tls = process.env.FLIGHT_TLS === "true"
const bearerToken = process.env.FLIGHT_BEARER_TOKEN

const url = `${tls ? "https" : "http"}://${host}:${String(port)}`

// ─────────────────────────────────────────────────────────────────────────────
// Example: Creating a Client
// ─────────────────────────────────────────────────────────────────────────────

const client = createFlightSqlClient({
  url,
  auth: bearerToken !== undefined ? { type: "bearer", token: bearerToken } : undefined
})

console.log(`Connected to: ${url}`)

// ─────────────────────────────────────────────────────────────────────────────
// Example: Simple Query
// ─────────────────────────────────────────────────────────────────────────────

async function simpleQuery(): Promise<void> {
  console.log("\n=== Simple Query ===")

  try {
    const table = await client.query("SELECT 1 AS value, 'hello' AS message")

    console.log(`Rows: ${String(table.numRows)}`)
    console.log(`Columns: ${table.schema.fields.map((f) => f.name).join(", ")}`)

    // Access values
    for (let i = 0; i < table.numRows; i++) {
      const row: Record<string, unknown> = {}
      for (let j = 0; j < table.schema.fields.length; j++) {
        row[table.schema.fields[j].name] = table.getChildAt(j)?.get(i)
      }
      console.log(`Row ${String(i)}: ${JSON.stringify(row)}`)
    }
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Streaming Results (requires database with data)
// ─────────────────────────────────────────────────────────────────────────────

export async function streamingQuery(): Promise<void> {
  console.log("\n=== Streaming Query ===")

  try {
    // Use queryBatches for large result sets to avoid loading all data into memory
    let totalRows = 0
    let batchCount = 0

    for await (const batch of client.queryBatches("SELECT * FROM large_table LIMIT 1000")) {
      batchCount++
      totalRows += batch.numRows
      console.log(`Batch ${String(batchCount)}: ${String(batch.numRows)} rows`)
    }

    console.log(`Total: ${String(totalRows)} rows in ${String(batchCount)} batches`)
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Prepared Statements (requires database)
// ─────────────────────────────────────────────────────────────────────────────

export async function preparedStatement(): Promise<void> {
  console.log("\n=== Prepared Statement ===")

  try {
    // Prepare a statement for reuse
    const stmt = await client.prepare("SELECT * FROM users WHERE id = ?")

    console.log(`Statement prepared (handle: ${String(stmt.handle.length)} bytes)`)

    // Execute the prepared statement
    // Note: Parameter binding depends on server support
    const table = await client.executePrepared(stmt)
    console.log(`Result: ${String(table.numRows)} rows`)

    // Always close prepared statements when done
    await client.closePreparedStatement(stmt)
    console.log("Statement closed")
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Transactions (requires database with write support)
// ─────────────────────────────────────────────────────────────────────────────

export async function transaction(): Promise<void> {
  console.log("\n=== Transaction ===")

  try {
    // Begin a transaction
    const txn = await client.beginTransaction()
    console.log(`Transaction started (id: ${String(txn.id.length)} bytes)`)

    try {
      // Execute updates within the transaction
      const result1 = await client.executeUpdate(
        "INSERT INTO accounts (id, balance) VALUES (1, 1000)",
        { transactionId: txn.id }
      )
      console.log(`Insert 1: ${String(result1.recordCount)} rows affected`)

      const result2 = await client.executeUpdate(
        "INSERT INTO accounts (id, balance) VALUES (2, 500)",
        { transactionId: txn.id }
      )
      console.log(`Insert 2: ${String(result2.recordCount)} rows affected`)

      // Commit the transaction
      await client.commit(txn)
      console.log("Transaction committed")
    } catch (error) {
      // Rollback on error
      await client.rollback(txn)
      console.log("Transaction rolled back")
      throw error
    }
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Database Metadata
// ─────────────────────────────────────────────────────────────────────────────

async function databaseMetadata(): Promise<void> {
  console.log("\n=== Database Metadata ===")

  try {
    // List catalogues
    const catalogs = await client.getCatalogs()
    console.log(`Catalogues: ${String(catalogs.numRows)}`)

    // List schemas
    const schemas = await client.getDbSchemas({})
    console.log(`Schemas: ${String(schemas.numRows)}`)

    // List tables
    const tables = await client.getTables({})
    console.log(`Tables: ${String(tables.numRows)}`)

    // Print first few tables
    for (let i = 0; i < Math.min(5, tables.numRows); i++) {
      const catalog = String(tables.getChildAt(0)?.get(i) ?? "")
      const schema = String(tables.getChildAt(1)?.get(i) ?? "")
      const tableName = String(tables.getChildAt(2)?.get(i) ?? "")
      const tableType = String(tables.getChildAt(3)?.get(i) ?? "")
      console.log(`  ${catalog}.${schema}.${tableName} (${tableType})`)
    }

    // Get table types
    const tableTypes = await client.getTableTypes()
    console.log(`\nTable types: ${String(tableTypes.numRows)}`)
    for (let i = 0; i < tableTypes.numRows; i++) {
      console.log(`  ${String(tableTypes.getChildAt(0)?.get(i))}`)
    }
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Execute Update (requires database with write support)
// ─────────────────────────────────────────────────────────────────────────────

export async function executeUpdate(): Promise<void> {
  console.log("\n=== Execute Update ===")

  try {
    // INSERT
    const insertResult = await client.executeUpdate(
      "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
    )
    console.log(`Inserted ${String(insertResult.recordCount)} row(s)`)

    // UPDATE
    const updateResult = await client.executeUpdate(
      "UPDATE users SET email = 'alice.new@example.com' WHERE name = 'Alice'"
    )
    console.log(`Updated ${String(updateResult.recordCount)} row(s)`)

    // DELETE
    const deleteResult = await client.executeUpdate("DELETE FROM users WHERE name = 'Alice'")
    console.log(`Deleted ${String(deleteResult.recordCount)} row(s)`)
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Error Handling
// ─────────────────────────────────────────────────────────────────────────────

function handleError(error: unknown): void {
  if (FlightConnectionError.isError(error)) {
    console.error(`connection error: ${error.message}`)
  } else if (FlightError.isError(error)) {
    console.error(`flight error: ${error.message}`)
  } else {
    throw error
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  try {
    await simpleQuery()
    await databaseMetadata()
    // These require a real database - uncomment when testing against a server:
    // await streamingQuery()
    // await preparedStatement()
    // await transaction()
    // await executeUpdate()
  } finally {
    client.close()
  }
}

main().catch(console.error)
