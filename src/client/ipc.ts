/**
 * Arrow IPC encoding and decoding utilities for Flight data streams.
 *
 * Flight uses Arrow IPC format to transfer data. FlightData messages contain:
 * - `dataHeader`: Arrow IPC message header (schema or record batch header)
 * - `dataBody`: Arrow IPC message body (data buffers)
 *
 * This module provides utilities to decode FlightData streams into Arrow
 * RecordBatches and Tables, and to encode RecordBatches for upload.
 */

import {
  type RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  type Schema,
  Table,
  type TypeMap
} from "apache-arrow"

import type { FlightData } from "../gen/arrow/flight/Flight_pb.js"

/**
 * Result of decoding a single FlightData message.
 */
export type DecodedFlightData =
  | { type: "schema"; schema: Schema }
  | { type: "batch"; batch: RecordBatch }
  | { type: "empty" }

/**
 * Decode a stream of FlightData messages into Arrow RecordBatches.
 *
 * The first message typically contains the schema. Subsequent messages
 * contain record batches.
 *
 * @param stream - Async iterable of FlightData messages
 * @yields RecordBatch objects
 *
 * @example
 * ```ts
 * const stream = client.doGetFlightData(ticket)
 * for await (const batch of decodeFlightDataStream(stream)) {
 *   console.log(`Received batch with ${batch.numRows} rows`)
 * }
 * ```
 */
export async function* decodeFlightDataStream<T extends TypeMap = TypeMap>(
  stream: AsyncIterable<FlightData>
): AsyncGenerator<RecordBatch<T>> {
  // Collect all IPC data from the stream
  const ipcChunks: Uint8Array[] = []

  for await (const data of stream) {
    // Skip empty messages
    if (data.dataHeader.length === 0 && data.dataBody.length === 0) {
      continue
    }

    // Reconstruct the IPC message by concatenating header and body
    // Arrow IPC streaming format: [continuation, metadata_size, metadata, body]
    const ipcMessage = createIpcMessage(data.dataHeader, data.dataBody)
    ipcChunks.push(ipcMessage)
  }

  if (ipcChunks.length === 0) {
    return
  }

  // Combine all chunks into a single buffer for the reader
  const totalLength = ipcChunks.reduce((sum, chunk) => sum + chunk.length, 0)
  const combined = new Uint8Array(totalLength)
  let offset = 0
  for (const chunk of ipcChunks) {
    combined.set(chunk, offset)
    offset += chunk.length
  }

  // Use RecordBatchReader to decode the IPC stream
  const reader = RecordBatchReader.from<T>(combined)

  for await (const batch of reader) {
    yield batch
  }
}

/**
 * Decode a stream of FlightData messages into an Arrow Table.
 *
 * Collects all record batches from the stream and combines them into a single table.
 *
 * @param stream - Async iterable of FlightData messages
 * @returns Arrow Table containing all data from the stream
 *
 * @example
 * ```ts
 * const stream = client.doGetFlightData(ticket)
 * const table = await decodeFlightDataToTable(stream)
 * console.log(`Received table with ${table.numRows} rows`)
 * ```
 */
export async function decodeFlightDataToTable<T extends TypeMap = TypeMap>(
  stream: AsyncIterable<FlightData>
): Promise<Table<T>> {
  const batches: RecordBatch<T>[] = []

  for await (const batch of decodeFlightDataStream<T>(stream)) {
    batches.push(batch)
  }

  if (batches.length === 0) {
    throw new Error("no record batches received from stream")
  }

  // Create table from the collected batches
  return new Table<T>(batches)
}

/**
 * Encode Arrow RecordBatches into FlightData messages for upload.
 *
 * Produces a stream of FlightData messages suitable for DoPut operations.
 * The first message contains the schema, followed by record batch messages.
 *
 * @param batches - Async iterable of RecordBatch objects to encode
 * @param schema - Arrow Schema for the data
 * @yields FlightData messages
 *
 * @example
 * ```ts
 * const batches = [recordBatch1, recordBatch2]
 * const flightData = encodeRecordBatchesToFlightData(batches, schema)
 * await client.doPut(descriptor, flightData)
 * ```
 */
export async function* encodeRecordBatchesToFlightData(
  batches: AsyncIterable<RecordBatch> | Iterable<RecordBatch>,
  schema: Schema
): AsyncGenerator<FlightData> {
  // Collect all batches first (we need to write them as a complete IPC stream)
  const batchArray: RecordBatch[] = []
  for await (const batch of batches) {
    batchArray.push(batch)
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

  if (ipcBytes.length > 0) {
    // Yield the complete IPC stream as a single FlightData message
    yield createFlightData(ipcBytes)
  }
}

/**
 * Encode an Arrow Table into FlightData messages for upload.
 *
 * @param table - Arrow Table to encode
 * @yields FlightData messages
 */
export async function* encodeTableToFlightData(table: Table): AsyncGenerator<FlightData> {
  yield* encodeRecordBatchesToFlightData(table.batches, table.schema)
}

/**
 * Create an IPC message from Flight data header and body.
 *
 * Arrow IPC streaming format uses a continuation marker, followed by
 * the metadata size, metadata, and body.
 */
function createIpcMessage(dataHeader: Uint8Array, dataBody: Uint8Array): Uint8Array {
  // Arrow IPC stream format:
  // - 4 bytes: continuation marker (0xFFFFFFFF)
  // - 4 bytes: metadata size (little-endian)
  // - N bytes: metadata (dataHeader, padded to 8-byte boundary)
  // - M bytes: body (dataBody)

  const continuationMarker = 0xffffffff
  const alignment = 8

  // Pad metadata to 8-byte boundary
  const metadataPaddedLength = Math.ceil(dataHeader.length / alignment) * alignment
  const padding = metadataPaddedLength - dataHeader.length

  const messageLength = 4 + 4 + metadataPaddedLength + dataBody.length
  const message = new Uint8Array(messageLength)
  const view = new DataView(message.buffer)

  let offset = 0

  // Write continuation marker
  view.setUint32(offset, continuationMarker, true)
  offset += 4

  // Write metadata size (original size, not padded)
  view.setInt32(offset, dataHeader.length, true)
  offset += 4

  // Write metadata (header)
  message.set(dataHeader, offset)
  offset += dataHeader.length

  // Add padding (zeros)
  offset += padding

  // Write body
  message.set(dataBody, offset)

  return message
}

/**
 * Create a FlightData message from raw IPC bytes.
 *
 * Parses the IPC message to extract header and body components,
 * creating a properly structured FlightData message.
 *
 * @param ipcBytes - Raw Arrow IPC bytes from RecordBatchStreamWriter
 * @returns FlightData message with separated header and body
 */
export function createFlightDataFromIpc(ipcBytes: Uint8Array): FlightData {
  // Parse the IPC message to extract header and body
  const { header, body } = parseIpcMessage(ipcBytes)

  return {
    $typeName: "arrow.flight.protocol.FlightData" as const,
    flightDescriptor: undefined,
    dataHeader: header,
    appMetadata: new Uint8Array(),
    dataBody: body
  }
}

// Internal alias for backward compatibility
const createFlightData = createFlightDataFromIpc

/**
 * Parse an IPC message into header and body components.
 *
 * Arrow IPC format:
 * - 4 bytes: continuation marker (0xFFFFFFFF)
 * - 4 bytes: metadata size (little-endian)
 * - N bytes: metadata (FlatBuffer, padded to 8-byte boundary)
 * - M bytes: body (data buffers)
 *
 * @internal Exported for testing purposes
 * @param ipcBytes - Raw IPC message bytes
 * @returns Object with header and body Uint8Arrays
 */
export function parseIpcMessage(ipcBytes: Uint8Array): { header: Uint8Array; body: Uint8Array } {
  if (ipcBytes.length < 8) {
    return { header: new Uint8Array(), body: ipcBytes }
  }

  const view = new DataView(ipcBytes.buffer, ipcBytes.byteOffset)

  // Check for continuation marker
  const marker = view.getUint32(0, true)
  if (marker !== 0xffffffff) {
    // No continuation marker - might be a different format
    return { header: ipcBytes, body: new Uint8Array() }
  }

  // Read metadata size
  const metadataSize = view.getInt32(4, true)
  if (metadataSize <= 0) {
    // End-of-stream marker
    return { header: new Uint8Array(), body: new Uint8Array() }
  }

  // Calculate padded metadata size (8-byte aligned)
  const alignment = 8
  const metadataPaddedSize = Math.ceil(metadataSize / alignment) * alignment

  // Extract header (metadata) and body
  const headerStart = 8
  const headerEnd = headerStart + metadataSize
  const bodyStart = 8 + metadataPaddedSize

  const header = ipcBytes.slice(headerStart, headerEnd)
  const body = bodyStart < ipcBytes.length ? ipcBytes.slice(bodyStart) : new Uint8Array()

  return { header, body }
}

/**
 * Get the schema from a FlightData stream.
 *
 * Decodes the stream to extract the schema from the first record batch.
 * Note: This consumes the stream.
 *
 * @param stream - Async iterable of FlightData messages
 * @returns The Arrow Schema from the stream
 */
export async function getSchemaFromFlightData(
  stream: AsyncIterable<FlightData>
): Promise<Schema | undefined> {
  // Use decodeFlightDataStream to get the first batch and return its schema
  for await (const batch of decodeFlightDataStream(stream)) {
    return batch.schema
  }

  return undefined
}
