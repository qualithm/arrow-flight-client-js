import { FlightClient } from "./flight-client.js"
import type { FlightClientOptions } from "./types.js"

/**
 * Factory function to create a FlightClient.
 *
 * This is the recommended way to create a Flight client as it provides
 * a cleaner API and better tree-shaking support.
 *
 * @example
 * ```ts
 * const client = createFlightClient({
 *   url: "https://flight.example.com:8815",
 *   headers: { "Authorization": "Bearer token" }
 * })
 *
 * try {
 *   const info = await client.getFlightInfo({ type: "path", path: ["my-dataset"] })
 *   console.log(info)
 * } finally {
 *   client.close()
 * }
 * ```
 *
 * @param options - Configuration options for the client
 * @returns A new FlightClient instance
 */
export function createFlightClient(options: FlightClientOptions): FlightClient {
  return new FlightClient(options)
}
