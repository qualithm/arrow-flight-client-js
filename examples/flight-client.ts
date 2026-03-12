/* eslint-disable no-console */
/**
 * FlightClient usage example.
 *
 * This example demonstrates basic Arrow Flight operations:
 * - Creating a client with authentication
 * - Listing available flights
 * - Retrieving flight metadata
 * - Fetching data with doGet
 * - Uploading data with doPut
 *
 * Run with: bun run examples/flight-client.ts
 *
 * Configure the server connection via environment variables:
 * - FLIGHT_HOST: Host address (default: localhost)
 * - FLIGHT_PORT: Port number (default: 50051)
 * - FLIGHT_TLS: Enable TLS (default: false)
 * - FLIGHT_BEARER_TOKEN: Bearer token for auth (optional)
 */

import {
  createFlightClient,
  decodeFlightDataToTable,
  encodeTableToFlightData,
  FlightConnectionError,
  type FlightData,
  FlightError
} from "@qualithm/arrow-flight-client"
import { tableFromArrays } from "apache-arrow"

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

const client = createFlightClient({
  url,
  // Bearer token authentication
  auth: bearerToken !== undefined ? { type: "bearer", token: bearerToken } : undefined
})

console.log(`Connected to: ${url}`)

// ─────────────────────────────────────────────────────────────────────────────
// Example: Listing Flights
// ─────────────────────────────────────────────────────────────────────────────

async function listFlights(): Promise<void> {
  console.log("\n=== Listing Flights ===")

  try {
    for await (const info of client.listFlights()) {
      const name = info.flightDescriptor?.path.join("/") ?? "unknown"
      console.log(`Flight: ${name}`)
      console.log(`  Records: ${String(info.totalRecords)}`)
      console.log(`  Bytes: ${String(info.totalBytes)}`)
    }
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Getting Flight Info
// ─────────────────────────────────────────────────────────────────────────────

async function getFlightInfo(): Promise<void> {
  console.log("\n=== Getting Flight Info ===")

  try {
    // Use a path descriptor to identify the flight
    const info = await client.getFlightInfo({
      type: "path",
      path: ["example", "dataset"]
    })

    console.log(`Total records: ${String(info.totalRecords)}`)
    console.log(`Endpoints: ${String(info.endpoint.length)}`)

    for (const endpoint of info.endpoint) {
      const locations = endpoint.location.map((l) => l.uri).join(", ") || "(local)"
      console.log(`  Endpoint: ${locations}`)
    }
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Fetching Data with doGet
// ─────────────────────────────────────────────────────────────────────────────

async function fetchData(): Promise<void> {
  console.log("\n=== Fetching Data ===")

  try {
    // First, get flight info to obtain a ticket
    const info = await client.getFlightInfo({
      type: "path",
      path: ["example", "dataset"]
    })

    if (info.endpoint.length === 0 || info.endpoint[0].ticket === undefined) {
      console.log("No data available")
      return
    }

    // Use the ticket to fetch data
    const { ticket } = info.endpoint[0]
    const table = await decodeFlightDataToTable(client.doGet(ticket))

    console.log(`Received ${String(table.numRows)} rows`)
    console.log(`Schema: ${table.schema.fields.map((f) => f.name).join(", ")}`)

    // Access data from the table
    for (let i = 0; i < Math.min(5, table.numRows); i++) {
      const row: Record<string, unknown> = {}
      for (const field of table.schema.fields) {
        const colIndex = table.schema.fields.indexOf(field)
        row[field.name] = table.getChildAt(colIndex)?.get(i)
      }
      console.log(`Row ${String(i)}: ${JSON.stringify(row)}`)
    }
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Uploading Data with doPut
// ─────────────────────────────────────────────────────────────────────────────

async function uploadData(): Promise<void> {
  console.log("\n=== Uploading Data ===")

  try {
    // Create sample data using Apache Arrow
    const table = tableFromArrays({
      id: Int32Array.from([1, 2, 3, 4, 5]),
      name: ["Alice", "Bob", "Charlie", "Diana", "Eve"],
      score: Float64Array.from([95.5, 87.2, 92.8, 78.9, 88.4])
    })

    // Convert to Flight data format
    const flightData = encodeTableToFlightData(table)

    // Add descriptor to first message for upload
    const descriptor = { type: 1, path: ["uploads", "scores"], cmd: new Uint8Array() } // type: 1 = PATH

    async function* withDescriptor(): AsyncGenerator<FlightData> {
      let first = true
      for await (const data of flightData) {
        if (first) {
          yield {
            ...data,
            flightDescriptor: descriptor
          } as unknown as typeof data
          first = false
        } else {
          yield data
        }
      }
    }

    // Upload to server
    for await (const result of client.doPut(withDescriptor())) {
      console.log(`Server acknowledged: ${String(result.appMetadata.length)} bytes`)
    }

    console.log(`Successfully uploaded ${String(table.numRows)} rows`)
  } catch (error) {
    handleError(error)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example: Listing Available Actions
// ─────────────────────────────────────────────────────────────────────────────

async function listActions(): Promise<void> {
  console.log("\n=== Available Actions ===")

  try {
    for await (const action of client.listActions()) {
      console.log(`${action.type}: ${action.description}`)
    }
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
    await listFlights()
    await listActions()
    await getFlightInfo()
    await fetchData()
    await uploadData()
  } finally {
    client.close()
  }
}

main().catch(console.error)
