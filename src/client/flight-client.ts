/* eslint-disable @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access */
// Disabled due to generated proto types using @ts-nocheck

import { createClient } from "@connectrpc/connect"
import { createGrpcTransport } from "@connectrpc/connect-node"

import { FlightService } from "../gen/arrow/flight/Flight_connect.js"
import type {
  ActionType,
  FlightDescriptor,
  FlightInfo,
  PollInfo,
  Result,
  SchemaResult
} from "../gen/arrow/flight/Flight_pb.js"
import { FlightConnectionError, FlightError, FlightServerError } from "./errors.js"
import {
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Generated proto types use @ts-nocheck
  readonly #client: any
  #closed = false

  constructor(options: FlightClientOptions) {
    this.#options = resolveOptions(options)

    const transport = createGrpcTransport({
      baseUrl: this.#options.url,
      httpVersion: "2"
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
   * Close the client and release resources.
   * After calling close, the client should not be used.
   */
  close(): void {
    this.#closed = true
  }

  /**
   * Get information about a specific flight.
   */
  async getFlightInfo(descriptor: FlightDescriptorInput): Promise<FlightInfo> {
    this.#assertOpen()
    try {
      return await this.#client.getFlightInfo(this.#toFlightDescriptor(descriptor), {
        headers: this.#options.headers
      })
    } catch (error) {
      throw this.#wrapError(error, "getFlightInfo")
    }
  }

  /**
   * Poll for updated flight information (useful for long-running queries).
   */
  async pollFlightInfo(descriptor: FlightDescriptorInput): Promise<PollInfo> {
    this.#assertOpen()
    try {
      return await this.#client.pollFlightInfo(this.#toFlightDescriptor(descriptor), {
        headers: this.#options.headers
      })
    } catch (error) {
      throw this.#wrapError(error, "pollFlightInfo")
    }
  }

  /**
   * Get the schema for a flight.
   */
  async getSchema(descriptor: FlightDescriptorInput): Promise<SchemaResult> {
    this.#assertOpen()
    try {
      return await this.#client.getSchema(this.#toFlightDescriptor(descriptor), {
        headers: this.#options.headers
      })
    } catch (error) {
      throw this.#wrapError(error, "getSchema")
    }
  }

  /**
   * List available flights matching the given criteria.
   */
  async *listFlights(criteria?: FlightCriteria): AsyncIterable<FlightInfo> {
    this.#assertOpen()
    try {
      const stream = this.#client.listFlights(
        { expression: criteria?.expression ?? new Uint8Array() },
        { headers: this.#options.headers }
      )
      for await (const info of stream) {
        yield info
      }
    } catch (error) {
      throw this.#wrapError(error, "listFlights")
    }
  }

  /**
   * List available actions supported by the server.
   */
  async *listActions(): AsyncIterable<ActionType> {
    this.#assertOpen()
    try {
      const stream = this.#client.listActions({}, { headers: this.#options.headers })
      for await (const action of stream) {
        yield action
      }
    } catch (error) {
      throw this.#wrapError(error, "listActions")
    }
  }

  /**
   * Execute a custom action on the server.
   */
  async *doAction(action: FlightAction): AsyncIterable<Result> {
    this.#assertOpen()
    try {
      const stream = this.#client.doAction(
        { type: action.type, body: action.body ?? new Uint8Array() },
        { headers: this.#options.headers }
      )
      for await (const result of stream) {
        yield result
      }
    } catch (error) {
      throw this.#wrapError(error, "doAction")
    }
  }

  /**
   * Retrieve flight data for the given ticket.
   * Returns an async iterable of FlightData messages.
   *
   * Note: FlightData contains raw Arrow IPC data. Use the Arrow IPC decoder
   * to parse the data into record batches.
   */
  async *doGet(ticket: FlightTicket): AsyncIterable<Uint8Array> {
    this.#assertOpen()
    try {
      const stream = this.#client.doGet(ticket, {
        headers: this.#options.headers
      })
      for await (const data of stream) {
        // Return the raw data body for downstream IPC decoding
        yield data.dataBody
      }
    } catch (error) {
      throw this.#wrapError(error, "doGet")
    }
  }

  // ── Private helpers ──────────────────────────────────────────────────

  #assertOpen(): void {
    if (this.#closed) {
      throw new FlightError("client is closed")
    }
  }

  #toFlightDescriptor(input: FlightDescriptorInput): Partial<FlightDescriptor> {
    if (input.type === "path") {
      return { type: 1, path: input.path } // PATH = 1
    }
    return { type: 2, cmd: input.cmd } // CMD = 2
  }

  #wrapError(error: unknown, operation: string): FlightError {
    if (FlightError.isError(error)) {
      return error
    }

    // Handle ConnectRPC errors
    if (error instanceof Error && "code" in error) {
      const connectError = error as Error & { code: string; rawMessage?: string }
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
