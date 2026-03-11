/**
 * FlightDescriptor helper functions for tests.
 *
 * @packageDocumentation
 */

import type { FlightDescriptorInput } from "../client/types.js"

/**
 * Create a path-based FlightDescriptor input.
 *
 * Path descriptors identify a dataset by a hierarchical path (like "db/schema/table").
 *
 * @param path - Path segments
 * @returns FlightDescriptorInput for path descriptor
 *
 * @example
 * ```ts
 * // Create descriptor for "test/integers"
 * const descriptor = pathDescriptor("test", "integers")
 *
 * // Use with client
 * const info = await client.getFlightInfo(descriptor)
 * ```
 */
export function pathDescriptor(...path: string[]): FlightDescriptorInput {
  return { type: "path", path }
}

/**
 * Create a command-based FlightDescriptor input.
 *
 * Command descriptors contain opaque bytes interpreted by the server.
 *
 * @param cmd - Command bytes or string (will be UTF-8 encoded)
 * @returns FlightDescriptorInput for command descriptor
 *
 * @example
 * ```ts
 * // Create descriptor with SQL query command
 * const descriptor = cmdDescriptor("SELECT * FROM users")
 *
 * // Create descriptor with binary command
 * const descriptor = cmdDescriptor(new Uint8Array([0x01, 0x02]))
 * ```
 */
export function cmdDescriptor(cmd: string | Uint8Array): FlightDescriptorInput {
  const cmdBytes = typeof cmd === "string" ? new TextEncoder().encode(cmd) : cmd
  return { type: "cmd", cmd: cmdBytes }
}
