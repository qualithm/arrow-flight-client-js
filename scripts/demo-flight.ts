#!/usr/bin/env bun
/**
 * Demo script showcasing FlightClient usage.
 *
 * Run with: bun run demo:flight
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
  createFlightClient,
  decodeFlightDataToTable,
  type FlightClient,
  FlightConnectionError,
  FlightError
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

function printHeader(title: string): void {
  console.log()
  console.log(`═══ ${title} ═══`)
  console.log()
}

function printSection(title: string): void {
  console.log(`── ${title} ──`)
}

// ── Demo Functions ────────────────────────────────────────────────────

async function demoListFlights(client: FlightClient): Promise<void> {
  printSection("Listing Available Flights")

  let count = 0
  for await (const info of client.listFlights()) {
    count++
    const descriptor = info.flightDescriptor

    let name = "unknown"
    if (descriptor?.path && descriptor.path.length > 0) {
      name = descriptor.path.join("/")
    }

    console.log(
      `  [${String(count)}] ${name} - ${String(info.totalRecords)} records, ${String(info.totalBytes)} bytes`
    )
  }

  if (count === 0) {
    console.log("  No flights found")
  } else {
    console.log()
    console.log(`  Total: ${String(count)} flight(s)`)
  }
}

async function demoListActions(client: FlightClient): Promise<void> {
  printSection("Listing Available Actions")

  let count = 0
  for await (const action of client.listActions()) {
    count++
    console.log(`  [${action.type}] ${action.description}`)
  }

  if (count === 0) {
    console.log("  No actions advertised")
  }
}

async function demoGetFlightInfo(client: FlightClient): Promise<void> {
  printSection("Getting Flight Info")

  // Get the first available flight
  let firstFlight = null
  for await (const info of client.listFlights()) {
    firstFlight = info
    break
  }

  if (!firstFlight?.flightDescriptor) {
    console.log("  No flights available to inspect")
    return
  }

  const { path } = firstFlight.flightDescriptor
  const descriptor = { type: "path" as const, path }

  console.log(`  Inspecting flight: ${path.join("/")}`)

  const info = await client.getFlightInfo(descriptor)

  console.log(`  Schema bytes: ${String(info.schema.length)}`)
  console.log(`  Total records: ${String(info.totalRecords)}`)
  console.log(`  Total bytes: ${String(info.totalBytes)}`)
  console.log(`  Endpoints: ${String(info.endpoint.length)}`)

  for (const endpoint of info.endpoint) {
    const locations = endpoint.location.map((l) => l.uri).join(", ") || "(local)"
    console.log(
      `    - ticket: ${String(endpoint.ticket?.ticket.length)} bytes, locations: ${locations}`
    )
  }
}

async function demoDoGet(client: FlightClient): Promise<void> {
  printSection("Retrieving Flight Data (doGet)")

  // Get the first available flight with data
  let flightInfo = null
  for await (const info of client.listFlights()) {
    if (info.totalRecords > 0 && info.endpoint.length > 0) {
      flightInfo = info
      break
    }
  }

  if (!flightInfo) {
    console.log("  No flights with data available")
    return
  }

  const path = flightInfo.flightDescriptor?.path.join("/") ?? "unknown"
  console.log(`  Fetching data from: ${path}`)

  const { ticket } = flightInfo.endpoint[0]
  if (!ticket) {
    console.log("  No ticket available for first endpoint")
    return
  }

  // Fetch and decode data
  const table = await decodeFlightDataToTable(client.doGet(ticket))

  console.log(`  Received: ${String(table.numRows)} rows`)
  console.log(`  Schema fields: ${table.schema.fields.map((f) => f.name).join(", ")}`)

  // Print first few rows
  if (table.numRows > 0) {
    console.log()
    console.log("  Sample data (first 3 rows):")
    const maxRows = Math.min(3, table.numRows)
    for (let i = 0; i < maxRows; i++) {
      const row: Record<string, unknown> = {}
      for (const field of table.schema.fields) {
        row[field.name] = table.getChildAt(table.schema.fields.indexOf(field))?.get(i)
      }
      console.log(`    ${JSON.stringify(row, jsonReplacer)}`)
    }
  }
}

// ── Main ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  printHeader("Arrow Flight Client Demo")

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
  const client = createFlightClient({ url, auth })

  try {
    // Authenticate if using basic auth
    if (auth?.type === "basic") {
      console.log("Authenticating...")
      await client.authenticate()
      console.log("Authentication successful")
    }

    // Run demos
    await demoListFlights(client)
    await demoListActions(client)
    await demoGetFlightInfo(client)
    await demoDoGet(client)

    printHeader("Demo Complete")
  } catch (error) {
    console.error()
    if (FlightConnectionError.isError(error)) {
      console.error(`Connection error: ${error.message}`)
      console.error("Ensure the Flight server is running and accessible")
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
