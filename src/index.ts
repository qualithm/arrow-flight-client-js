/**
 * Unified Arrow Flight and Flight SQL client for JavaScript and TypeScript runtimes.
 *
 * @packageDocumentation
 */

// Clients
export {
  createFlightClient,
  createFlightSqlClient,
  FlightClient,
  FlightSqlClient
} from "./client/index.js"

// Errors
export {
  FlightAuthError,
  FlightCancelledError,
  FlightConnectionError,
  FlightError,
  FlightServerError,
  FlightTimeoutError
} from "./client/index.js"

// IPC utilities
export {
  createFlightDataFromIpc,
  decodeFlightDataStream,
  decodeFlightDataToTable,
  encodeRecordBatchesToFlightData,
  encodeTableToFlightData,
  getSchemaFromFlightData,
  parseIpcMessage
} from "./client/index.js"

// Constants
export { DEFAULT_TIMEOUT_MS } from "./client/index.js"

// Client types
export type {
  AuthOptions,
  BasicAuthCredentials,
  DecodedFlightData,
  ExecuteQueryOptions,
  ExecuteUpdateOptions,
  FlightAction,
  FlightClientOptions,
  FlightCriteria,
  FlightDescriptorInput,
  FlightTicket,
  PreparedStatement,
  ResolvedFlightClientOptions,
  TlsOptions,
  Transaction,
  UpdateResult
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
