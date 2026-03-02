// Client
export { createFlightClient } from "./create-flight-client.js"
export { FlightClient } from "./flight-client.js"

// Errors
export {
  FlightAuthError,
  FlightCancelledError,
  FlightConnectionError,
  FlightError,
  FlightServerError,
  FlightTimeoutError
} from "./errors.js"

// Types
export type {
  FlightAction,
  FlightClientOptions,
  FlightCriteria,
  FlightDescriptorInput,
  FlightTicket,
  ResolvedFlightClientOptions
} from "./types.js"
export { DEFAULT_TIMEOUT_MS } from "./types.js"
