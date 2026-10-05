import { FlightClient } from "./flight-client.js"
import type { FlightClientOptions } from "./types.js"

/**
 * Create a FlightClient.
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
