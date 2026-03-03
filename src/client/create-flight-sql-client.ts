import { FlightSqlClient } from "./flight-sql-client.js"
import type { FlightClientOptions } from "./types.js"

/**
 * Factory function to create a FlightSqlClient.
 *
 * This is the recommended way to create a Flight SQL client as it provides
 * a cleaner API and better tree-shaking support.
 *
 * @example
 * ```ts
 * const client = createFlightSqlClient({
 *   url: "https://flight.example.com:8815",
 *   auth: { type: "bearer", token: "my-token" }
 * })
 *
 * try {
 *   const table = await client.query("SELECT * FROM users")
 *   console.log(`Found ${table.numRows} users`)
 * } finally {
 *   client.close()
 * }
 * ```
 *
 * @param options - Configuration options for the client
 * @returns A new FlightSqlClient instance
 */
export function createFlightSqlClient(options: FlightClientOptions): FlightSqlClient {
  return new FlightSqlClient(options)
}
