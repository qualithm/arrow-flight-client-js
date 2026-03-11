/**
 * Integration test configuration.
 *
 * Configure the Arrow Flight server connection via environment variables:
 * - FLIGHT_HOST: Host address (default: localhost)
 * - FLIGHT_PORT: Port number (default: 50051)
 * - FLIGHT_TLS: Enable TLS (default: false)
 */

import { createConnection } from "node:net"

const host = process.env.FLIGHT_HOST ?? "localhost"
const port = parseInt(process.env.FLIGHT_PORT ?? "50051", 10)
const tls = process.env.FLIGHT_TLS === "true"
const bearerToken = process.env.FLIGHT_BEARER_TOKEN

export const config = {
  host,
  port,
  tls,
  bearerToken,

  /** Fully qualified URL for FlightClient */
  url: `${tls ? "https" : "http"}://${host}:${String(port)}`,

  // Test credentials (configure for your Flight server)
  credentials: {
    admin: { username: "admin", password: "admin123" },
    reader: { username: "reader", password: "reader123" },
    invalid: { username: "invalid", password: "wrong" }
  },

  // Test flight paths (configure for your Flight server)
  flights: {
    integers: ["test", "integers"],
    strings: ["test", "strings"],
    allTypes: ["test", "all-types"],
    empty: ["test", "empty"],
    large: ["test", "large"],
    nested: ["test", "nested"]
  },

  // Test tables (Flight SQL exposes flights as tables)
  tables: {
    integers: "test.integers",
    strings: "test.strings",
    allTypes: "test.all_types",
    empty: "test.empty",
    large: "test.large",
    nested: "test.nested"
  },

  // Server-specific catalog name (configure for your Flight SQL server)
  catalog: process.env.FLIGHT_CATALOG ?? "default"
} as const

/**
 * Check if Flight server is reachable by attempting a TCP connection.
 * Returns true if the server accepts a connection within the timeout.
 */
export async function isFlightAvailable(timeoutMs = 3000): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ host: config.host, port: config.port, timeout: timeoutMs })

    socket.on("connect", () => {
      socket.destroy()
      resolve(true)
    })

    socket.on("timeout", () => {
      socket.destroy()
      resolve(false)
    })

    socket.on("error", () => {
      socket.destroy()
      resolve(false)
    })
  })
}
