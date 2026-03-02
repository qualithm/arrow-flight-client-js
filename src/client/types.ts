/**
 * Configuration options for creating a Flight client.
 */
export type FlightClientOptions = {
  /**
   * The base URL of the Flight server.
   * Should include protocol (http:// or https://).
   * @example "https://flight.example.com:8815"
   */
  url: string

  /**
   * Optional headers to include with every request.
   * Useful for authentication tokens or custom metadata.
   */
  headers?: Record<string, string>

  /**
   * Request timeout in milliseconds.
   * @default 30000
   */
  timeoutMs?: number
}

/**
 * Resolved configuration with defaults applied.
 */
export type ResolvedFlightClientOptions = Required<Pick<FlightClientOptions, "url" | "timeoutMs">> &
  Pick<FlightClientOptions, "headers">

/**
 * Default timeout for Flight client requests (30 seconds).
 */
export const DEFAULT_TIMEOUT_MS = 30_000

/**
 * Applies default values to client options.
 */
export function resolveOptions(options: FlightClientOptions): ResolvedFlightClientOptions {
  return {
    url: options.url,
    headers: options.headers,
    timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  }
}

/**
 * Descriptor for a Flight — either a path or a command.
 */
export type FlightDescriptorInput =
  | { type: "path"; path: string[] }
  | { type: "cmd"; cmd: Uint8Array }

/**
 * Criteria for listing flights.
 */
export type FlightCriteria = {
  /** Filter expression (server-specific) */
  expression?: Uint8Array
}

/**
 * Ticket for retrieving a specific flight stream.
 */
export type FlightTicket = {
  ticket: Uint8Array
}

/**
 * Action request for DoAction RPC.
 */
export type FlightAction = {
  type: string
  body?: Uint8Array
}
