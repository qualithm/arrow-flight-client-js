/**
 * Test helpers for code that uses Arrow Flight Client, exported at
 * `@qualithm/arrow-flight-client/testing`.
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
