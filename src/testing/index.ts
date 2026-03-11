/**
 * Testing utilities for Arrow Flight Client.
 *
 * This subpath export (`@qualithm/arrow-flight-client/testing`) provides:
 *
 * - **Table helpers** — Create Arrow Tables and RecordBatches for tests
 * - **FlightData builders** — Construct FlightData messages without protobuf boilerplate
 * - **Mock streams** — Create async iterables for testing stream consumers
 * - **Descriptor helpers** — Build FlightDescriptor inputs easily
 *
 * @example
 * ```ts
 * import {
 *   // Table helpers
 *   createTestTable, createTestBatch,
 *   // FlightData builders
 *   createEmptyFlightData, tableToFlightData,
 *   // Mock streams
 *   asyncIterable, emptyStream,
 *   // Descriptor helpers
 *   pathDescriptor, cmdDescriptor
 * } from "@qualithm/arrow-flight-client/testing"
 *
 * // Create test data
 * const table = createTestTable({ id: [1, 2, 3], value: [1.1, 2.2, 3.3] })
 *
 * // Build FlightData from table
 * const flightData = await tableToFlightData(table)
 *
 * // Create mock streams
 * const stream = asyncIterable(flightData)
 *
 * // Build descriptors
 * const descriptor = pathDescriptor("test", "integers")
 * ```
 *
 * @packageDocumentation
 */

// Table helpers
export {
  createFloatTable,
  createIntegerTable,
  createStringTable,
  createTestBatch,
  createTestTable,
  type TestTableData
} from "./helpers.js"

// FlightData builders
export {
  batchesToFlightData,
  collectFlightData,
  createEmptyFlightData,
  tableToFlightData
} from "./builders.js"

// Mock streams
export {
  asyncIterable,
  concatStreams,
  delayedIterable,
  emptyStream,
  errorAfter
} from "./streams.js"

// Descriptor helpers
export { cmdDescriptor, pathDescriptor } from "./descriptors.js"

// Re-export proto schemas for advanced test fixture construction
export {
  FlightDataSchema,
  FlightDescriptor_DescriptorType,
  FlightDescriptorSchema,
  FlightEndpointSchema,
  FlightInfoSchema,
  TicketSchema
} from "../gen/arrow/flight/Flight_pb.js"
