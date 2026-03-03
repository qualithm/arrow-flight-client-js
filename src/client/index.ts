// Client
export { createFlightClient } from "./create-flight-client.js"
export { createFlightSqlClient } from "./create-flight-sql-client.js"
export { FlightClient } from "./flight-client.js"
export type {
  ExecuteQueryOptions,
  ExecuteUpdateOptions,
  PreparedStatement,
  Transaction,
  UpdateResult
} from "./flight-sql-client.js"
export { FlightSqlClient } from "./flight-sql-client.js"

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
