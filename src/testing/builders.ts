/**
 * FlightData builders for constructing test messages.
 *
 * @packageDocumentation
 */

import { create } from "@bufbuild/protobuf"
import { type RecordBatch, RecordBatchStreamWriter, type Schema, type Table } from "apache-arrow"

import { type FlightData, FlightDataSchema } from "../gen/arrow/flight/Flight_pb.js"

/**
 * Create an empty FlightData message (keep-alive).
 *
 * @returns Empty FlightData message
 */
export function createEmptyFlightData(): FlightData {
  return create(FlightDataSchema, {
    dataHeader: new Uint8Array(0),
    dataBody: new Uint8Array(0),
    appMetadata: new Uint8Array(0)
  })
}

/**
 * Collect all FlightData messages from an async iterable into an array.
 *
 * Useful for testing stream consumers by materializing the stream.
 *
 * @param stream - Async iterable of FlightData messages
 * @returns Array of FlightData messages
 *
 * @example
 * ```ts
 * const table = createTestTable({ id: [1, 2, 3] })
 * const flightData = await collectFlightData(encodeTableToFlightData(table))
 * expect(flightData.length).toBeGreaterThan(0)
 * ```
 */
export async function collectFlightData(stream: AsyncIterable<FlightData>): Promise<FlightData[]> {
  const result: FlightData[] = []

  for await (const data of stream) {
    result.push(data)
  }

  return result
}

/**
 * Convert an Arrow Table to an array of FlightData messages.
 *
 * Encodes the table as IPC stream format and wraps in FlightData.
 *
 * @param table - Arrow Table to convert
 * @returns Array of FlightData messages
 *
 * @example
 * ```ts
 * const table = createTestTable({ id: [1, 2, 3], value: [1.1, 2.2, 3.3] })
 * const flightData = await tableToFlightData(table)
 * ```
 */
export async function tableToFlightData(table: Table): Promise<FlightData[]> {
  return Promise.resolve(batchesToFlightData(table.batches, table.schema))
}

/**
 * Convert Arrow RecordBatches to an array of FlightData messages.
 *
 * @param batches - RecordBatches to convert
 * @param schema - Arrow Schema for the data
 * @returns Array of FlightData messages
 */
export function batchesToFlightData(
  batches: Iterable<RecordBatch> | RecordBatch[],
  schema: Schema
): FlightData[] {
  const batchArray = Array.isArray(batches) ? batches : [...batches]

  if (batchArray.length === 0) {
    return []
  }

  // Create an IPC stream writer and write all batches
  const writer = new RecordBatchStreamWriter()
  writer.reset(undefined, schema)

  for (const batch of batchArray) {
    writer.write(batch)
  }
  writer.finish()

  // Get the complete IPC stream
  const ipcBytes = writer.toUint8Array(true)

  if (ipcBytes.length === 0) {
    return []
  }

  // Wrap IPC bytes in FlightData
  // Note: In production, Flight typically splits schema and batches into separate messages.
  // For testing, a single message with the complete IPC stream works.
  return [
    create(FlightDataSchema, {
      dataHeader: ipcBytes,
      dataBody: new Uint8Array(0),
      appMetadata: new Uint8Array(0)
    })
  ]
}
