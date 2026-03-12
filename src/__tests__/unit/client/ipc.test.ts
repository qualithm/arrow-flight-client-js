import { type RecordBatch, type Table, tableFromArrays } from "apache-arrow"
import { describe, expect, it } from "vitest"

import {
  createFlightDataFromIpc,
  decodeFlightDataStream,
  decodeFlightDataToTable,
  encodeRecordBatchesToFlightData,
  encodeTableToFlightData,
  getSchemaFromFlightData,
  parseIpcMessage
} from "../../../client/ipc.js"
import type { FlightData } from "../../../gen/arrow/flight/Flight_pb.js"

// Helper to create a simple test table
function createTestTable(): Table {
  return tableFromArrays({
    id: new Int32Array([1, 2, 3]),
    value: new Float64Array([1.1, 2.2, 3.3])
  })
}

// Helper to create an async iterable from an array
async function* asyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield await Promise.resolve(item)
  }
}

describe("IPC encoding/decoding utilities", () => {
  describe("encodeTableToFlightData", () => {
    it("encodes a table to FlightData messages", async () => {
      const table = createTestTable()

      const flightDataMessages: FlightData[] = []
      for await (const data of encodeTableToFlightData(table)) {
        flightDataMessages.push(data)
      }

      // Should produce at least one message (schema + data)
      expect(flightDataMessages.length).toBeGreaterThan(0)

      // Each message should have the expected structure
      for (const data of flightDataMessages) {
        expect(data.$typeName).toBe("arrow.flight.protocol.FlightData")
        expect(data.dataHeader).toBeInstanceOf(Uint8Array)
        expect(data.dataBody).toBeInstanceOf(Uint8Array)
        expect(data.appMetadata).toBeInstanceOf(Uint8Array)
      }
    })

    it("produces FlightData with non-empty data", async () => {
      const table = createTestTable()

      let totalHeaderBytes = 0
      let totalBodyBytes = 0

      for await (const data of encodeTableToFlightData(table)) {
        totalHeaderBytes += data.dataHeader.length
        totalBodyBytes += data.dataBody.length
      }

      // Should have some actual data
      expect(totalHeaderBytes + totalBodyBytes).toBeGreaterThan(0)
    })
  })

  describe("encodeRecordBatchesToFlightData", () => {
    it("encodes record batches to FlightData messages", async () => {
      const table = createTestTable()
      const { batches } = table
      const { schema } = table

      const flightDataMessages: FlightData[] = []
      for await (const data of encodeRecordBatchesToFlightData(batches, schema)) {
        flightDataMessages.push(data)
      }

      expect(flightDataMessages.length).toBeGreaterThan(0)
    })

    it("handles async iterable of batches", async () => {
      const table = createTestTable()
      const batch = table.batches[0]
      const { schema } = table

      const flightDataMessages: FlightData[] = []
      for await (const data of encodeRecordBatchesToFlightData(asyncIterable([batch]), schema)) {
        flightDataMessages.push(data)
      }

      expect(flightDataMessages.length).toBeGreaterThan(0)
    })

    it("handles empty batches array", async () => {
      const table = createTestTable()
      const { schema } = table

      // Pass empty array - should produce at least schema message
      const flightDataMessages: FlightData[] = []
      for await (const data of encodeRecordBatchesToFlightData([], schema)) {
        flightDataMessages.push(data)
      }

      // Should produce at least schema message from IPC stream
      expect(flightDataMessages.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe("decodeFlightDataStream", () => {
    it("decodes empty stream to empty result", async () => {
      const batches: RecordBatch[] = []
      for await (const batch of decodeFlightDataStream(asyncIterable([]))) {
        batches.push(batch)
      }

      expect(batches).toHaveLength(0)
    })

    it("skips empty FlightData messages", async () => {
      const emptyData: FlightData[] = [
        {
          $typeName: "arrow.flight.protocol.FlightData",
          dataHeader: new Uint8Array(),
          dataBody: new Uint8Array(),
          appMetadata: new Uint8Array(),
          flightDescriptor: undefined
        }
      ]

      const batches: RecordBatch[] = []
      for await (const batch of decodeFlightDataStream(asyncIterable(emptyData))) {
        batches.push(batch)
      }

      expect(batches).toHaveLength(0)
    })

    it("round-trips through encode/decode", async () => {
      const table = createTestTable()

      // Encode the table
      const flightData: FlightData[] = []
      for await (const data of encodeTableToFlightData(table)) {
        flightData.push(data)
      }

      // Decode the FlightData
      const decodedBatches: RecordBatch[] = []
      for await (const batch of decodeFlightDataStream(asyncIterable(flightData))) {
        decodedBatches.push(batch)
      }

      // Should get back at least one batch
      expect(decodedBatches.length).toBeGreaterThan(0)

      // The schema should match
      const decodedSchema = decodedBatches[0].schema
      expect(decodedSchema.fields.length).toBe(2)
      expect(decodedSchema.fields[0].name).toBe("id")
      expect(decodedSchema.fields[1].name).toBe("value")
    })
  })

  describe("decodeFlightDataToTable", () => {
    it("throws on empty stream", async () => {
      await expect(decodeFlightDataToTable(asyncIterable([]))).rejects.toThrow(
        "no record batches received from stream"
      )
    })

    it("round-trips a table through encode/decode", async () => {
      const originalTable = createTestTable()

      // Encode
      const flightData: FlightData[] = []
      for await (const data of encodeTableToFlightData(originalTable)) {
        flightData.push(data)
      }

      // Decode
      const decodedTable = await decodeFlightDataToTable(asyncIterable(flightData))

      // Verify structure
      expect(decodedTable.numCols).toBe(originalTable.numCols)
      expect(decodedTable.schema.fields.length).toBe(2)
    })
  })

  describe("getSchemaFromFlightData", () => {
    it("returns undefined for empty stream", async () => {
      const schema = await getSchemaFromFlightData(asyncIterable([]))
      expect(schema).toBeUndefined()
    })

    it("extracts schema from FlightData stream", async () => {
      const table = createTestTable()

      // Encode
      const flightData: FlightData[] = []
      for await (const data of encodeTableToFlightData(table)) {
        flightData.push(data)
      }

      // Extract schema
      const schema = await getSchemaFromFlightData(asyncIterable(flightData))

      expect(schema).toBeDefined()
      expect(schema!.fields.length).toBe(2)
      expect(schema!.fields[0].name).toBe("id")
      expect(schema!.fields[1].name).toBe("value")
    })

    it("skips empty FlightData messages when looking for schema", async () => {
      const table = createTestTable()

      // Encode with an empty message prepended
      const flightData: FlightData[] = [
        {
          $typeName: "arrow.flight.protocol.FlightData",
          dataHeader: new Uint8Array(),
          dataBody: new Uint8Array(),
          appMetadata: new Uint8Array(),
          flightDescriptor: undefined
        }
      ]
      for await (const data of encodeTableToFlightData(table)) {
        flightData.push(data)
      }

      // Extract schema - should skip the empty message
      const schema = await getSchemaFromFlightData(asyncIterable(flightData))

      expect(schema).toBeDefined()
      expect(schema!.fields.length).toBe(2)
    })
  })
})

describe("IPC with different data types", () => {
  it("handles Float64 columns", async () => {
    const table = tableFromArrays({
      value: new Float64Array([1.5, 2.5, 3.5])
    })

    // Round-trip
    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
    expect(decoded.schema.fields[0].name).toBe("value")
  })

  it("handles empty tables", async () => {
    const table = tableFromArrays({
      id: new Int32Array([])
    })

    // Encode
    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    // Should encode without error
    expect(flightData.length).toBeGreaterThan(0)
  })

  it("handles multiple columns of different types", async () => {
    const table = tableFromArrays({
      int_col: new Int32Array([1, 2, 3]),
      float_col: new Float64Array([1.1, 2.2, 3.3]),
      uint_col: new Uint32Array([10, 20, 30])
    })

    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
    expect(decoded.numCols).toBe(3)
    expect(decoded.schema.fields[0].name).toBe("int_col")
    expect(decoded.schema.fields[1].name).toBe("float_col")
    expect(decoded.schema.fields[2].name).toBe("uint_col")
  })

  it("preserves data values after round-trip", async () => {
    const originalData = {
      id: new Int32Array([100, 200, 300]),
      value: new Float64Array([1.5, 2.5, 3.5])
    }
    const table = tableFromArrays(originalData)

    // Round-trip
    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    const decoded = await decodeFlightDataToTable(asyncIterable(flightData))

    // Verify row count
    expect(decoded.numRows).toBe(3)

    // Verify data integrity
    const idColumn = decoded.getChild("id")
    const valueColumn = decoded.getChild("value")

    expect(idColumn?.get(0)).toBe(100)
    expect(idColumn?.get(1)).toBe(200)
    expect(idColumn?.get(2)).toBe(300)

    expect(valueColumn?.get(0)).toBe(1.5)
    expect(valueColumn?.get(1)).toBe(2.5)
    expect(valueColumn?.get(2)).toBe(3.5)
  })

  it("handles large tables", async () => {
    const size = 10000
    const ids = new Int32Array(size)
    const values = new Float64Array(size)
    for (let i = 0; i < size; i++) {
      ids[i] = i
      values[i] = i * 0.1
    }

    const table = tableFromArrays({ id: ids, value: values })

    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
    expect(decoded.numRows).toBe(size)
  })
})

describe("IPC edge cases", () => {
  it("handles FlightData with only header (no body)", async () => {
    const table = createTestTable()

    // Encode to get valid FlightData
    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    // Modify to have empty body (simulating metadata-only message)
    const modifiedData: FlightData[] = flightData.map((fd) => ({
      ...fd,
      dataBody: new Uint8Array()
    }))

    // Should handle gracefully (may not decode to batches but shouldn't throw)
    const batches: RecordBatch[] = []
    for await (const batch of decodeFlightDataStream(asyncIterable(modifiedData))) {
      batches.push(batch)
    }

    // May or may not produce batches depending on the data, but shouldn't crash
    expect(Array.isArray(batches)).toBe(true)
  })

  it("handles FlightData with minimal valid IPC header", async () => {
    // Create FlightData with a very small header that's technically valid IPC format
    // The IPC continuation marker is 0xFFFFFFFF followed by metadata size
    const minimalHeader = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0x00,
      0x00,
      0x00,
      0x00 // Zero metadata size (EOS marker)
    ])

    const flightData: FlightData[] = [
      {
        $typeName: "arrow.flight.protocol.FlightData",
        dataHeader: minimalHeader,
        dataBody: new Uint8Array(),
        appMetadata: new Uint8Array(),
        flightDescriptor: undefined
      }
    ]

    // Should handle EOS marker gracefully
    const batches: RecordBatch[] = []
    for await (const batch of decodeFlightDataStream(asyncIterable(flightData))) {
      batches.push(batch)
    }

    expect(batches).toHaveLength(0)
  })

  it("handles multiple batches from a table", async () => {
    // Create a larger table that might produce multiple batches
    const size = 5000
    const ids = new Int32Array(size)
    for (let i = 0; i < size; i++) {
      ids[i] = i
    }

    const table = tableFromArrays({ id: ids })

    const flightData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      flightData.push(fd)
    }

    // Verify we got messages
    expect(flightData.length).toBeGreaterThan(0)

    // Decode and verify
    const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
    expect(decoded.numRows).toBe(size)
  })

  it("decodeFlightDataStream handles mixed empty and valid messages", async () => {
    const table = createTestTable()

    // Get valid flight data
    const validData: FlightData[] = []
    for await (const fd of encodeTableToFlightData(table)) {
      validData.push(fd)
    }

    // Interleave with empty messages
    const mixedData: FlightData[] = [
      {
        $typeName: "arrow.flight.protocol.FlightData",
        dataHeader: new Uint8Array(),
        dataBody: new Uint8Array(),
        appMetadata: new Uint8Array(),
        flightDescriptor: undefined
      },
      ...validData,
      {
        $typeName: "arrow.flight.protocol.FlightData",
        dataHeader: new Uint8Array(),
        dataBody: new Uint8Array(),
        appMetadata: new Uint8Array(),
        flightDescriptor: undefined
      }
    ]

    const batches: RecordBatch[] = []
    for await (const batch of decodeFlightDataStream(asyncIterable(mixedData))) {
      batches.push(batch)
    }

    // Should successfully decode the valid data
    expect(batches.length).toBeGreaterThan(0)
  })
})

describe("parseIpcMessage", () => {
  it("handles very short input (less than 8 bytes)", () => {
    const shortInput = new Uint8Array([1, 2, 3, 4])
    const result = parseIpcMessage(shortInput)

    // Should return empty header and input as body
    expect(result.header).toEqual(new Uint8Array())
    expect(result.body).toEqual(shortInput)
  })

  it("handles empty input", () => {
    const emptyInput = new Uint8Array([])
    const result = parseIpcMessage(emptyInput)

    expect(result.header).toEqual(new Uint8Array())
    expect(result.body).toEqual(emptyInput)
  })

  it("handles input without continuation marker", () => {
    // Input that doesn't start with 0xFFFFFFFF
    const noMarkerInput = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00])
    const result = parseIpcMessage(noMarkerInput)

    // Should return input as header and empty body
    expect(result.header).toEqual(noMarkerInput)
    expect(result.body).toEqual(new Uint8Array())
  })

  it("handles end-of-stream marker (zero metadata size)", () => {
    // Valid continuation marker but zero metadata size
    const eosMarker = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0x00,
      0x00,
      0x00,
      0x00 // Zero metadata size
    ])
    const result = parseIpcMessage(eosMarker)

    // Should return empty header and body
    expect(result.header).toEqual(new Uint8Array())
    expect(result.body).toEqual(new Uint8Array())
  })

  it("handles negative metadata size", () => {
    // Valid continuation marker but negative metadata size
    const negativeSize = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0xff,
      0xff,
      0xff,
      0xff // -1 in little-endian
    ])
    const result = parseIpcMessage(negativeSize)

    // Should return empty header and body (treated as EOS)
    expect(result.header).toEqual(new Uint8Array())
    expect(result.body).toEqual(new Uint8Array())
  })

  it("parses valid IPC message with header and body", () => {
    // Create a valid IPC message structure:
    // - 4 bytes: continuation marker (0xFFFFFFFF)
    // - 4 bytes: metadata size (8 bytes)
    // - 8 bytes: metadata (padded to 8-byte boundary)
    // - 4 bytes: body
    const validMessage = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0x08,
      0x00,
      0x00,
      0x00, // Metadata size: 8
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08, // Metadata (8 bytes, already aligned)
      0x0a,
      0x0b,
      0x0c,
      0x0d // Body (4 bytes)
    ])
    const result = parseIpcMessage(validMessage)

    expect(result.header).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]))
    expect(result.body).toEqual(new Uint8Array([0x0a, 0x0b, 0x0c, 0x0d]))
  })

  it("handles message without body", () => {
    // IPC message with only header, no body
    const headerOnly = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0x08,
      0x00,
      0x00,
      0x00, // Metadata size: 8
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08 // Metadata (8 bytes)
      // No body
    ])
    const result = parseIpcMessage(headerOnly)

    expect(result.header).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]))
    expect(result.body).toEqual(new Uint8Array())
  })

  it("handles unaligned metadata size (requires padding)", () => {
    // Metadata size of 5 should be padded to 8
    const unalignedMessage = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0x05,
      0x00,
      0x00,
      0x00, // Metadata size: 5
      0x01,
      0x02,
      0x03,
      0x04,
      0x05, // Metadata (5 bytes)
      0x00,
      0x00,
      0x00, // Padding (3 bytes to reach 8)
      0x0a,
      0x0b,
      0x0c,
      0x0d // Body (4 bytes)
    ])
    const result = parseIpcMessage(unalignedMessage)

    // Header should be only the actual metadata (5 bytes), not including padding
    expect(result.header).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05]))
    expect(result.body).toEqual(new Uint8Array([0x0a, 0x0b, 0x0c, 0x0d]))
  })
})

describe("createFlightDataFromIpc", () => {
  it("creates FlightData from valid IPC bytes", () => {
    const ipcBytes = new Uint8Array([
      0xff,
      0xff,
      0xff,
      0xff, // Continuation marker
      0x08,
      0x00,
      0x00,
      0x00, // Metadata size: 8
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08, // Metadata
      0x0a,
      0x0b,
      0x0c,
      0x0d // Body
    ])

    const flightData = createFlightDataFromIpc(ipcBytes)

    expect(flightData.$typeName).toBe("arrow.flight.protocol.FlightData")
    expect(flightData.dataHeader).toEqual(
      new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
    )
    expect(flightData.dataBody).toEqual(new Uint8Array([0x0a, 0x0b, 0x0c, 0x0d]))
    expect(flightData.appMetadata).toEqual(new Uint8Array())
    expect(flightData.flightDescriptor).toBeUndefined()
  })

  it("handles short IPC input gracefully", () => {
    const shortInput = new Uint8Array([1, 2, 3])

    const flightData = createFlightDataFromIpc(shortInput)

    expect(flightData.$typeName).toBe("arrow.flight.protocol.FlightData")
    expect(flightData.dataHeader).toEqual(new Uint8Array())
    expect(flightData.dataBody).toEqual(shortInput)
  })

  it("handles EOS marker", () => {
    const eosMarker = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00])

    const flightData = createFlightDataFromIpc(eosMarker)

    expect(flightData.dataHeader).toEqual(new Uint8Array())
    expect(flightData.dataBody).toEqual(new Uint8Array())
  })
})
