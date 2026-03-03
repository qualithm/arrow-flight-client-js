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

// IPC utilities
export type { DecodedFlightData } from "./ipc.js"
export {
  createFlightDataFromIpc,
  decodeFlightDataStream,
  decodeFlightDataToTable,
  encodeRecordBatchesToFlightData,
  encodeTableToFlightData,
  getSchemaFromFlightData,
  parseIpcMessage
} from "./ipc.js"

// Types
export type {
  AuthOptions,
  BasicAuthCredentials,
  FlightAction,
  FlightClientOptions,
  FlightCriteria,
  FlightDescriptorInput,
  FlightTicket,
  ResolvedFlightClientOptions,
  TlsOptions
} from "./types.js"
export { DEFAULT_TIMEOUT_MS } from "./types.js"
