import { create, toBinary } from "@bufbuild/protobuf"
import { type Client, createClient } from "@connectrpc/connect"
import { createGrpcTransport } from "@connectrpc/connect-node"

import {
  type ActionType,
  BasicAuthSchema,
  type FlightData,
  type FlightDescriptor,
  FlightDescriptorSchema,
  type FlightInfo,
  FlightService,
  type HandshakeResponse,
  type PollInfo,
  type PutResult,
  type Result,
  type SchemaResult
} from "../gen/arrow/flight/Flight_pb.js"
import { FlightAuthError, FlightConnectionError, FlightError, FlightServerError } from "./errors.js"
import {
  type AuthOptions,
  type FlightAction,
  type FlightClientOptions,
  type FlightCriteria,
  type FlightDescriptorInput,
  type FlightTicket,
  type ResolvedFlightClientOptions,
  resolveOptions
} from "./types.js"

/**
 * Low-level Arrow Flight client for communicating with Flight servers.
 *
 * This client provides access to all core Flight RPC methods.
 * For SQL operations, use `FlightSqlClient` instead.
 *
 * @example
 * ```ts
 * const client = new FlightClient({ url: "https://flight.example.com:8815" })
 *
 * const info = await client.getFlightInfo({ type: "cmd", cmd: myCommand })
 * for await (const flight of client.listFlights()) {
 *   console.log(flight)
 * }
 *
 * client.close()
 * ```
 */
export class FlightClient {
  readonly #options: ResolvedFlightClientOptions
  readonly #client: Client<typeof FlightService>
  #closed = false
  #authenticated = false
  #authToken: string | undefined
  /** Credentials most recently resolved from `authProvider`, if configured. */
  #currentAuth: AuthOptions | undefined
  /** Coalesces concurrent refreshes so a burst of rejections triggers one handshake. */
  #refreshInFlight: Promise<string | undefined> | undefined

  constructor(options: FlightClientOptions) {
    this.#options = resolveOptions(options)

    // Build node options with TLS configuration if provided
    const nodeOptions = this.#buildNodeOptions()

    const transport = createGrpcTransport({
      baseUrl: this.#options.url,
      nodeOptions
    })

    this.#client = createClient(FlightService, transport)
  }

  /**
   * The base URL of the Flight server.
   */
  get url(): string {
    return this.#options.url
  }

  /**
   * Whether the client has been closed.
   */
  get closed(): boolean {
    return this.#closed
  }

  /**
   * Whether the client has been authenticated via handshake.
   */
  get authenticated(): boolean {
    return this.#authenticated
  }

  /**
   * Close the client and release resources.
   * After calling close, the client should not be used.
   */
  close(): void {
    this.#closed = true
  }

  /**
   * Perform Flight Handshake authentication.
   *
   * This method is automatically called for clients configured with `auth: { type: "basic" }`.
   * For custom handshake payloads, call this method directly with raw bytes.
   *
   * @param payload - Raw handshake payload (defaults to BasicAuth if auth.type is "basic")
   * @returns The authentication token from the server
   */
  async handshake(payload?: Uint8Array): Promise<string> {
    this.#assertOpen()

    // Use provided payload or build from basic auth credentials
    let handshakePayload = payload
    const auth = this.#effectiveAuth()
    if (!handshakePayload && auth?.type === "basic") {
      const basicAuth = create(BasicAuthSchema, {
        username: auth.credentials.username,
        password: auth.credentials.password
      })
      handshakePayload = toBinary(BasicAuthSchema, basicAuth)
    }

    if (!handshakePayload) {
      throw new FlightError(
        "no handshake payload provided and no basic auth credentials configured"
      )
    }

    try {
      // Create async iterable with single handshake request
      const requests = async function* (): AsyncGenerator<
        { protocolVersion: bigint; payload: Uint8Array },
        void,
        unknown
      > {
        yield await Promise.resolve({
          protocolVersion: 0n,
          payload: handshakePayload
        })
      }

      const stream = this.#client.handshake(requests(), {
        headers: this.#getRequestHeaders()
      })

      let response: HandshakeResponse | undefined
      for await (const msg of stream) {
        response = msg
        break // Only need first response
      }

      if (!response) {
        throw new FlightAuthError("handshake failed: no response from server")
      }

      // Extract token from response payload (typically Bearer token)
      const token = new TextDecoder().decode(response.payload)
      this.#authToken = token
      this.#authenticated = true

      return token
    } catch (error) {
      if (FlightError.isError(error)) {
        throw error
      }
      throw this.#wrapError(error, "handshake")
    }
  }

  /**
   * Authenticate with the server using configured credentials.
   *
   * For basic auth, this calls the Handshake RPC.
   * For bearer auth, no action is needed (token is sent in headers).
   *
   * When an `authProvider` is configured, this re-resolves credentials through
   * it, so callers who know a credential has rotated can adopt it eagerly
   * rather than waiting for the server to reject a call.
   *
   * @returns The authentication token (if applicable)
   */
  async authenticate(): Promise<string | undefined> {
    this.#assertOpen()

    if (this.#options.authProvider !== undefined) {
      return this.#refreshAuth()
    }

    if (this.#options.auth?.type === "basic") {
      return this.handshake()
    }

    if (this.#options.auth?.type === "bearer") {
      this.#authenticated = true
      return this.#options.auth.token
    }

    // No auth configured
    return undefined
  }

  /**
   * Get information about a specific flight.
   *
   * @param descriptor - Flight descriptor identifying the dataset
   * @returns Flight information including schema and endpoints
   */
  async getFlightInfo(descriptor: FlightDescriptorInput): Promise<FlightInfo> {
    this.#assertOpen()
    return this.#withReauth("getFlightInfo", async () =>
      this.#client.getFlightInfo(this.#toFlightDescriptor(descriptor), {
        headers: this.#getRequestHeaders()
      })
    )
  }

  /**
   * Poll for updated flight information (useful for long-running queries).
   *
   * @param descriptor - Flight descriptor identifying the dataset
   * @returns Poll information with progress and updated flight info
   */
  async pollFlightInfo(descriptor: FlightDescriptorInput): Promise<PollInfo> {
    this.#assertOpen()
    return this.#withReauth("pollFlightInfo", async () =>
      this.#client.pollFlightInfo(this.#toFlightDescriptor(descriptor), {
        headers: this.#getRequestHeaders()
      })
    )
  }

  /**
   * Get the schema for a flight.
   *
   * @param descriptor - Flight descriptor identifying the dataset
   * @returns Schema result containing the Arrow schema bytes
   */
  async getSchema(descriptor: FlightDescriptorInput): Promise<SchemaResult> {
    this.#assertOpen()
    return this.#withReauth("getSchema", async () =>
      this.#client.getSchema(this.#toFlightDescriptor(descriptor), {
        headers: this.#getRequestHeaders()
      })
    )
  }

  /**
   * List available flights matching the given criteria.
   *
   * @param criteria - Optional filter criteria for listing flights
   * @yields FlightInfo for each matching flight
   */
  async *listFlights(criteria?: FlightCriteria): AsyncIterable<FlightInfo> {
    this.#assertOpen()
    yield* this.#streamWithReauth("listFlights", () =>
      this.#client.listFlights(
        { expression: criteria?.expression ?? new Uint8Array() },
        { headers: this.#getRequestHeaders() }
      )
    )
  }

  /**
   * List available actions supported by the server.
   *
   * @yields ActionType describing each available action
   */
  async *listActions(): AsyncIterable<ActionType> {
    this.#assertOpen()
    yield* this.#streamWithReauth("listActions", () =>
      this.#client.listActions({}, { headers: this.#getRequestHeaders() })
    )
  }

  /**
   * Execute a custom action on the server.
   *
   * @param action - The action to execute (type and optional body)
   * @yields Result messages from the server
   */
  async *doAction(action: FlightAction): AsyncIterable<Result> {
    this.#assertOpen()
    yield* this.#streamWithReauth("doAction", () =>
      this.#client.doAction(
        { type: action.type, body: action.body ?? new Uint8Array() },
        { headers: this.#getRequestHeaders() }
      )
    )
  }

  /**
   * Retrieve flight data for the given ticket.
   * Returns an async iterable of FlightData messages.
   *
   * Use the IPC decoding utilities to convert FlightData to Arrow RecordBatches:
   * - `decodeFlightDataStream()` - decode to RecordBatch stream
   * - `decodeFlightDataToTable()` - decode to a single Table
   *
   * @param ticket - The ticket identifying the data to retrieve
   * @yields FlightData messages containing Arrow IPC data
   *
   * @example
   * ```ts
   * import { decodeFlightDataStream } from "@qualithm/arrow-flight-client"
   *
   * const stream = client.doGet(ticket)
   * for await (const batch of decodeFlightDataStream(stream)) {
   *   console.log(`Received batch with ${batch.numRows} rows`)
   * }
   * ```
   */
  async *doGet(ticket: FlightTicket): AsyncIterable<FlightData> {
    this.#assertOpen()
    yield* this.#streamWithReauth("doGet", () =>
      this.#client.doGet(ticket, {
        headers: this.#getRequestHeaders()
      })
    )
  }

  /**
   * Upload data to the server.
   * Returns an async iterable of PutResult messages containing server acknowledgements.
   *
   * Use the IPC encoding utilities to create FlightData from Arrow data:
   * - `encodeRecordBatchesToFlightData()` - encode RecordBatch stream
   * - `encodeTableToFlightData()` - encode a Table
   *
   * @param data - Async iterable of FlightData messages to upload (include descriptor in first message)
   * @yields PutResult messages from the server
   *
   * @example
   * ```ts
   * import { encodeTableToFlightData } from "@qualithm/arrow-flight-client"
   *
   * const descriptor = { type: "path", path: ["my", "table"] }
   * const flightData = encodeTableToFlightData(table)
   *
   * // Add descriptor to first message
   * async function* withDescriptor() {
   *   let first = true
   *   for await (const data of flightData) {
   *     if (first) {
   *       yield { ...data, flightDescriptor: descriptor }
   *       first = false
   *     } else {
   *       yield data
   *     }
   *   }
   * }
   *
   * for await (const result of client.doPut(withDescriptor())) {
   *   console.log("Server acknowledged:", result.appMetadata)
   * }
   * ```
   */
  async *doPut(data: AsyncIterable<FlightData>): AsyncIterable<PutResult> {
    this.#assertOpen()
    // No re-auth retry here: the caller's stream has already been partly drained
    // by the time a rejection arrives, and it cannot be replayed.
    await this.#ensureAuth()
    try {
      const stream = this.#client.doPut(data, {
        headers: this.#getRequestHeaders()
      })
      for await (const result of stream) {
        yield result
      }
    } catch (error) {
      throw this.#wrapError(error, "doPut")
    }
  }

  // ── Private helpers ──────────────────────────────────────────────────

  /** Throws if the client has been closed. */
  #assertOpen(): void {
    if (this.#closed) {
      throw new FlightError("client is closed")
    }
  }

  /** Converts a FlightDescriptorInput to the proto FlightDescriptor format. */
  #toFlightDescriptor(input: FlightDescriptorInput): FlightDescriptor {
    if (input.type === "path") {
      return create(FlightDescriptorSchema, { type: 1, path: input.path }) // PATH = 1
    }
    return create(FlightDescriptorSchema, { type: 2, cmd: input.cmd }) // CMD = 2
  }

  /** Builds Node.js HTTP/2 options including TLS configuration. */
  #buildNodeOptions(): Record<string, unknown> {
    const nodeOptions: Record<string, unknown> = { ...this.#options.nodeOptions }

    // Apply TLS options if configured
    if (this.#options.tls) {
      const { tls } = this.#options
      if (tls.cert !== undefined) {
        nodeOptions.cert = tls.cert
      }
      if (tls.key !== undefined) {
        nodeOptions.key = tls.key
      }
      if (tls.ca !== undefined) {
        nodeOptions.ca = tls.ca
      }
      if (tls.passphrase !== undefined && tls.passphrase !== "") {
        nodeOptions.passphrase = tls.passphrase
      }
      if (tls.rejectUnauthorized !== undefined) {
        nodeOptions.rejectUnauthorized = tls.rejectUnauthorized
      }
    }

    return nodeOptions
  }

  /** Returns headers for requests, including auth token if authenticated. */
  #getRequestHeaders(): Record<string, string> {
    const headers: Record<string, string> = { ...this.#options.headers }

    // A provider-resolved bearer token supersedes the one baked in at construction
    const auth = this.#currentAuth
    if (auth?.type === "bearer" && auth.token !== "") {
      headers.Authorization = `Bearer ${auth.token}`
    }

    // Add auth token if authenticated via handshake
    if (this.#authToken !== undefined && this.#authToken !== "") {
      headers.Authorization = `Bearer ${this.#authToken}`
    }

    return headers
  }

  /** The credentials in force: provider-resolved if available, else static config. */
  #effectiveAuth(): AuthOptions | undefined {
    return this.#currentAuth ?? this.#options.auth
  }

  /** Resolves credentials before the first request, so it isn't spent on a rejection. */
  async #ensureAuth(): Promise<void> {
    if (this.#options.authProvider === undefined || this.#currentAuth !== undefined) {
      return
    }
    await this.#refreshAuth()
  }

  /** Re-resolves credentials through the provider, coalescing concurrent callers. */
  async #refreshAuth(): Promise<string | undefined> {
    if (this.#refreshInFlight === undefined) {
      this.#refreshInFlight = this.#resolveFromProvider().finally(() => {
        this.#refreshInFlight = undefined
      })
    }
    return this.#refreshInFlight
  }

  /** Calls the provider and applies the credentials it returns. */
  async #resolveFromProvider(): Promise<string | undefined> {
    const provider = this.#options.authProvider
    if (provider === undefined) {
      return undefined
    }

    const auth = await provider()
    this.#currentAuth = auth
    this.#authToken = undefined
    this.#authenticated = false

    if (auth.type === "basic") {
      return this.handshake()
    }

    if (auth.type === "bearer") {
      this.#authenticated = true
      return auth.token
    }

    return undefined
  }

  /** Whether a failed call is worth retrying with freshly resolved credentials. */
  #canReauth(error: FlightError): boolean {
    return this.#options.authProvider !== undefined && FlightAuthError.isError(error)
  }

  /** Runs a unary call, retrying once with fresh credentials if the server rejects it. */
  async #withReauth<T>(operation: string, call: () => Promise<T>): Promise<T> {
    await this.#ensureAuth()

    try {
      return await call()
    } catch (error) {
      const wrapped = this.#wrapError(error, operation)
      if (!this.#canReauth(wrapped)) {
        throw wrapped
      }
      await this.#refreshAuth()
    }

    try {
      return await call()
    } catch (error) {
      throw this.#wrapError(error, operation)
    }
  }

  /**
   * Runs a server-streaming call, retrying once with fresh credentials if the
   * server rejects it.
   *
   * Only retries when the rejection arrives before any message, since a
   * consumer that has already seen part of a stream would otherwise see the
   * opening messages twice.
   */
  async *#streamWithReauth<T>(operation: string, start: () => AsyncIterable<T>): AsyncIterable<T> {
    await this.#ensureAuth()

    let yielded = false
    try {
      for await (const item of start()) {
        yielded = true
        yield item
      }
      return
    } catch (error) {
      const wrapped = this.#wrapError(error, operation)
      if (yielded || !this.#canReauth(wrapped)) {
        throw wrapped
      }
      await this.#refreshAuth()
    }

    try {
      for await (const item of start()) {
        yield item
      }
    } catch (error) {
      throw this.#wrapError(error, operation)
    }
  }

  /** Wraps errors in appropriate FlightError subclasses based on error type. */
  #wrapError(error: unknown, operation: string): FlightError {
    if (FlightError.isError(error)) {
      return error
    }

    // Handle ConnectRPC errors
    if (error instanceof Error && "code" in error) {
      const connectError = error as Error & { code: string; rawMessage?: string }

      // Check for authentication-related errors
      if (connectError.code === "UNAUTHENTICATED" || connectError.code === "PERMISSION_DENIED") {
        return new FlightAuthError(`${operation} failed: ${connectError.message}`, error)
      }

      return new FlightServerError(
        `${operation} failed: ${connectError.message}`,
        connectError.code,
        connectError.rawMessage,
        error
      )
    }

    // Handle connection errors
    if (error instanceof Error && error.message.includes("ECONNREFUSED")) {
      return new FlightConnectionError(
        `failed to connect to ${this.#options.url}`,
        this.#options.url,
        error
      )
    }

    // Generic error wrapping
    return new FlightError(
      `${operation} failed: ${error instanceof Error ? error.message : String(error)}`,
      error
    )
  }
}
