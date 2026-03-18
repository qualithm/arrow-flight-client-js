/**
 * FlightClient example.
 *
 * Demonstrates basic Arrow Flight operations: listing flights, retrieving
 * metadata, fetching data with doGet, and uploading data with doPut.
 *
 * Requires a running Arrow Flight server.
 * Set `FLIGHT_HOST`, `FLIGHT_PORT`, `FLIGHT_TLS`, and `FLIGHT_BEARER_TOKEN`
 * to configure the connection.
 *
 * @example
 * ```bash
 * bun run examples/flight-client.ts
 * ```
 */

/* eslint-disable no-console */

import {
  createFlightClient,
  decodeFlightDataToTable,
  encodeTableToFlightData,
  FlightConnectionError,
  type FlightData,
  FlightError
} from "@qualithm/arrow-flight-client"
import { tableFromArrays } from "apache-arrow"

const host = process.env.FLIGHT_HOST ?? "localhost"
const port = parseInt(process.env.FLIGHT_PORT ?? "50051", 10)
const tls = process.env.FLIGHT_TLS === "true"
const bearerToken = process.env.FLIGHT_BEARER_TOKEN

const url = `${tls ? "https" : "http"}://${host}:${String(port)}`

const client = createFlightClient({
  url,
  // Bearer token authentication
  auth: bearerToken !== undefined ? { type: "bearer", token: bearerToken } : undefined
})

console.log(`Connected to: ${url}`)

// List all available flights on the server.
async function listFlights(): Promise<void> {
  console.log("--- Listing flights ---")

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

// Retrieve metadata for a specific flight.
async function getFlightInfo(): Promise<void> {
  console.log("\n--- Getting flight info ---")

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

// Fetch data using a ticket from flight info.
async function fetchData(): Promise<void> {
  console.log("\n--- Fetching data ---")

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

// Upload an Arrow table to the server.
async function uploadData(): Promise<void> {
  console.log("\n--- Uploading data ---")

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

// List actions supported by the server.
async function listActions(): Promise<void> {
  console.log("\n--- Available actions ---")

  try {
    for await (const action of client.listActions()) {
      console.log(`${action.type}: ${action.description}`)
    }
  } catch (error) {
    handleError(error)
  }
}

// Handle flight errors with type narrowing.
function handleError(error: unknown): void {
  if (FlightConnectionError.isError(error)) {
    console.error(`connection error: ${error.message}`)
  } else if (FlightError.isError(error)) {
    console.error(`flight error: ${error.message}`)
  } else {
    throw error
  }
}

async function main(): Promise<void> {
  console.log("=== Flight Client ===\n")
  try {
    await listFlights()
    await listActions()
    await getFlightInfo()
    await fetchData()
    await uploadData()
  } finally {
    client.close()
  }
  console.log("\nDone.")
}

main().catch(console.error)
