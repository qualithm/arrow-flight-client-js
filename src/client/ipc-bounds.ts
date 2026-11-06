/**
 * Bounds checks on an Arrow IPC message header, run before apache-arrow decodes it. The decoder
 * trusts every length in the flatbuffer and allocates from it, so one corrupt length exhausts
 * memory instead of failing. This walk reads only what it can verify lies inside the bytes it was
 * given.
 */

import { FlightDecodeError } from "./errors.js"

/** `MessageHeader` union tags (Arrow `Message.fbs`). */
const HEADER_SCHEMA = 1
const HEADER_DICTIONARY_BATCH = 2
const HEADER_RECORD_BATCH = 3

/** `FieldNode` and `Buffer` are both two `long`s. */
const STRUCT_16 = 16
/** Each element of a vector of tables or strings is a 4-byte offset. */
const OFFSET = 4
/** Nesting deeper than any real schema means a corrupt or hostile header. */
const MAX_FIELD_DEPTH = 64

class HeaderBoundsError extends Error {}

/** A flatbuffer reader that refuses any read outside its bytes. */
class Reader {
  private readonly bytes: Uint8Array
  private readonly view: DataView

  constructor(bytes: Uint8Array) {
    this.bytes = bytes
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  }

  private need(at: number, size: number): void {
    if (!Number.isSafeInteger(at) || at < 0 || at + size > this.bytes.byteLength) {
      throw new HeaderBoundsError()
    }
  }

  u8(at: number): number {
    this.need(at, 1)
    return this.view.getUint8(at)
  }

  u16(at: number): number {
    this.need(at, 2)
    return this.view.getUint16(at, true)
  }

  u32(at: number): number {
    this.need(at, 4)
    return this.view.getUint32(at, true)
  }

  i64(at: number): number {
    this.need(at, 8)
    const value = this.view.getBigInt64(at, true)
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new HeaderBoundsError()
    }
    return Number(value)
  }

  /** The position of field `index` in the table at `table`, or undefined when absent. */
  field(table: number, index: number): number | undefined {
    this.need(table, 4)
    const vtable = table - this.view.getInt32(table, true)
    const vtableSize = this.u16(vtable)
    const slot = 4 + 2 * index
    if (slot + 2 > vtableSize) {
      return undefined
    }
    const offset = this.u16(vtable + slot)
    return offset === 0 ? undefined : table + offset
  }

  /** Follow the offset stored at `at` to the table, vector or string it points to. */
  deref(at: number): number {
    return at + this.u32(at)
  }

  /** Check the vector at `vector` fits its bytes; returns its element count. */
  vector(vector: number, elementSize: number): number {
    const count = this.u32(vector)
    this.need(vector + 4, count * elementSize)
    return count
  }
}

function checkFields(reader: Reader, vector: number, depth: number): void {
  if (depth > MAX_FIELD_DEPTH) {
    throw new HeaderBoundsError()
  }
  const count = reader.vector(vector, OFFSET)
  for (let index = 0; index < count; index++) {
    const field = reader.deref(vector + 4 + OFFSET * index)
    const name = reader.field(field, 0)
    if (name !== undefined) {
      reader.vector(reader.deref(name), 1)
    }
    const children = reader.field(field, 5)
    if (children !== undefined) {
      checkFields(reader, reader.deref(children), depth + 1)
    }
    const metadata = reader.field(field, 6)
    if (metadata !== undefined) {
      checkKeyValues(reader, reader.deref(metadata))
    }
  }
}

function checkKeyValues(reader: Reader, vector: number): void {
  const count = reader.vector(vector, OFFSET)
  for (let index = 0; index < count; index++) {
    const pair = reader.deref(vector + 4 + OFFSET * index)
    for (const slot of [0, 1]) {
      const text = reader.field(pair, slot)
      if (text !== undefined) {
        reader.vector(reader.deref(text), 1)
      }
    }
  }
}

function checkRecordBatch(reader: Reader, batch: number, bodyLength: number): void {
  const nodes = reader.field(batch, 1)
  if (nodes !== undefined) {
    reader.vector(reader.deref(nodes), STRUCT_16)
  }
  const buffers = reader.field(batch, 2)
  if (buffers === undefined) {
    return
  }
  const vector = reader.deref(buffers)
  const count = reader.vector(vector, STRUCT_16)
  for (let index = 0; index < count; index++) {
    const region = vector + 4 + STRUCT_16 * index
    if (reader.i64(region) + reader.i64(region + 8) > bodyLength) {
      throw new HeaderBoundsError()
    }
  }
}

/** A header that is itself the IPC end-of-stream marker, which the decoder reads as a clean end. */
function isEndOfStream(header: Uint8Array): boolean {
  if (header.byteLength !== 8) {
    return false
  }
  const view = new DataView(header.buffer, header.byteOffset, header.byteLength)
  return view.getUint32(0, true) === 0xffffffff && view.getInt32(4, true) === 0
}

/**
 * Refuse an IPC message header whose lengths don't fit the bytes that arrived with it.
 *
 * @param header - the FlightData's `dataHeader`: one flatbuffer-encoded `Message`
 * @param body - the FlightData's `dataBody`
 * @throws {FlightDecodeError} When any length in the header points outside the header or body
 */
export function checkIpcHeader(header: Uint8Array, body: Uint8Array): void {
  if (isEndOfStream(header)) {
    return
  }
  try {
    const reader = new Reader(header)
    const message = reader.deref(0)
    const typeAt = reader.field(message, 1)
    const headerAt = reader.field(message, 2)
    if (typeAt === undefined || headerAt === undefined) {
      return
    }
    const type = reader.u8(typeAt)
    const bodyAt = reader.field(message, 3)
    const bodyLength = bodyAt === undefined ? 0 : reader.i64(bodyAt)
    if (bodyLength > body.byteLength) {
      throw new HeaderBoundsError()
    }
    const table = reader.deref(headerAt)
    if (type === HEADER_RECORD_BATCH) {
      checkRecordBatch(reader, table, bodyLength)
    } else if (type === HEADER_DICTIONARY_BATCH) {
      const data = reader.field(table, 1)
      if (data !== undefined) {
        checkRecordBatch(reader, reader.deref(data), bodyLength)
      }
    } else if (type === HEADER_SCHEMA) {
      const fields = reader.field(table, 1)
      if (fields !== undefined) {
        checkFields(reader, reader.deref(fields), 0)
      }
      const metadata = reader.field(table, 2)
      if (metadata !== undefined) {
        checkKeyValues(reader, reader.deref(metadata))
      }
    }
    const metadata = reader.field(message, 4)
    if (metadata !== undefined) {
      checkKeyValues(reader, reader.deref(metadata))
    }
  } catch (error) {
    // Every read is bounds-checked, so a bounds failure is the only thing that can land here.
    throw new FlightDecodeError("flight data header has a length outside its bytes", error)
  }
}
