import type * as http2 from "node:http2"

/**
 * TLS options for mTLS authentication.
 * These are passed directly to Node.js HTTP/2 client.
 */
export type TlsOptions = {
  /**
   * Client certificate (PEM format).
   */
  cert?: string | Buffer

  /**
   * Client private key (PEM format).
   */
  key?: string | Buffer

  /**
   * CA certificate(s) for server verification (PEM format).
   * If not provided, uses default system CA.
   */
  ca?: string | Buffer | (string | Buffer)[]

  /**
   * Passphrase for the private key.
   */
  passphrase?: string

  /**
   * Whether to reject unauthorised certificates.
   * @default true
   */
  rejectUnauthorized?: boolean
}

/**
 * Basic authentication credentials for Flight Handshake.
 */
export type BasicAuthCredentials = {
  username: string
  password: string
}

/**
 * Authentication options for Flight client.
 * Only one authentication method should be specified.
 */
export type AuthOptions =
  | { type: "bearer"; token: string }
  | { type: "basic"; credentials: BasicAuthCredentials }
  | { type: "none" }

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
   * Useful for custom metadata. For authentication, prefer using the `auth` option.
   */
  headers?: Record<string, string>

  /**
   * Request timeout in milliseconds.
   * @default 30000
   */
  timeoutMs?: number

  /**
   * Authentication options.
   * - `bearer`: Sets Authorization header with Bearer token
   * - `basic`: Uses Flight Handshake RPC with BasicAuth
   * - `none`: No authentication (default)
   */
  auth?: AuthOptions

  /**
   * TLS options for mTLS authentication.
   * Passed to the underlying Node.js HTTP/2 client.
   */
  tls?: TlsOptions

  /**
   * Additional Node.js HTTP/2 session options.
   * Advanced use only.
   */
  nodeOptions?: http2.ClientSessionOptions | http2.SecureClientSessionOptions
}

/**
 * Resolved configuration with defaults applied.
 */
export type ResolvedFlightClientOptions = Required<Pick<FlightClientOptions, "url" | "timeoutMs">> &
  Pick<FlightClientOptions, "headers" | "auth" | "tls" | "nodeOptions">

/**
 * Default timeout for Flight client requests (30 seconds).
 */
export const DEFAULT_TIMEOUT_MS = 30_000

/**
 * Applies default values to client options.
 */
export function resolveOptions(options: FlightClientOptions): ResolvedFlightClientOptions {
  // Build headers, adding Bearer token if auth type is bearer
  let headers = options.headers ? { ...options.headers } : undefined
  if (options.auth?.type === "bearer") {
    headers = {
      ...headers,
      Authorization: `Bearer ${options.auth.token}`
    }
  }

  return {
    url: options.url,
    headers,
    timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    auth: options.auth,
    tls: options.tls,
    nodeOptions: options.nodeOptions
  }
}

/**
 * Descriptor for a Flight — either a path or a command.
 *
 * - `path`: Identifies a dataset by hierarchical path (e.g., ["database", "table"])
 * - `cmd`: Identifies a dataset by an opaque command (e.g., serialised SQL query)
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
  /** Opaque ticket bytes returned by the server in FlightEndpoint. */
  ticket: Uint8Array
}

/**
 * Action request for DoAction RPC.
 */
export type FlightAction = {
  /** Action type identifier (e.g., "DropDataset", "CancelFlightInfo"). */
  type: string
  /** Optional action body containing serialised parameters. */
  body?: Uint8Array
}
