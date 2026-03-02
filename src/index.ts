/**
 * Unified Arrow Flight client for JavaScript and TypeScript runtimes.
 *
 * @packageDocumentation
 */

// Client
export {
  createFlightClient,
  // Constants
  DEFAULT_TIMEOUT_MS,
  FlightAuthError,
  FlightCancelledError,
  FlightClient,
  FlightConnectionError,
  // Errors
  FlightError,
  FlightServerError,
  FlightTimeoutError
} from "./client/index.js"

// Types
export type {
  FlightAction,
  FlightClientOptions,
  FlightCriteria,
  FlightDescriptorInput,
  FlightTicket,
  ResolvedFlightClientOptions
} from "./client/index.js"

// Re-export generated proto types for advanced usage
export type {
  ActionType,
  FlightData,
  FlightDescriptor,
  FlightEndpoint,
  FlightInfo,
  PollInfo,
  Result,
  SchemaResult,
  Ticket
} from "./gen/arrow/flight/Flight_pb.js"

// Legacy exports (to be removed in future major version)
export { greet, type GreetOptions } from "./greet.js"
