import {
  type Data,
  type DataType,
  Field,
  Int32,
  makeData,
  RecordBatch,
  Schema,
  Struct,
  Table,
  vectorFromArray
} from "apache-arrow"
import { describe, expect, it } from "vitest"

import { FlightDecodeError } from "../../../client/errors.js"
import { encodeTableToFlightData } from "../../../client/ipc.js"
import { checkIpcHeader } from "../../../client/ipc-bounds.js"
import type { FlightData } from "../../../gen/arrow/flight/Flight_pb.js"

async function messagesOf(table: Table): Promise<FlightData[]> {
  const messages: FlightData[] = []
  for await (const message of encodeTableToFlightData(table)) {
    messages.push(message)
  }
  return messages
}

/** A one-column table whose column is `depth` structs deep around an Int32. */
function nested(depth: number): Table {
  let type: DataType = new Int32()
  let data: Data = makeData({ type: new Int32(), length: 1, data: Int32Array.of(1) })
  for (let level = 0; level < depth; level++) {
    const struct = new Struct([new Field(`l${String(level)}`, type)])
    data = makeData({ type: struct, length: 1, nullCount: 0, children: [data] })
    type = struct
  }
  const schema = new Schema([new Field("root", type)])
  return new Table(
    schema,
    new RecordBatch(
      schema,
      makeData({ type: new Struct(schema.fields), length: 1, nullCount: 0, children: [data] })
    )
  )
}

describe("checkIpcHeader", () => {
  it("accepts every message of a valid stream with schema and field metadata", async () => {
    const id = vectorFromArray([1, 2], new Int32())
    const field = new Field("id", new Int32(), false, new Map([["unit", "count"]]))
    const schema = new Schema([field], new Map([["source", "test"]]))
    const table = new Table(
      schema,
      new RecordBatch(
        schema,
        makeData({
          type: new Struct(schema.fields),
          length: 2,
          nullCount: 0,
          children: [id.data[0]]
        })
      )
    )
    for (const message of await messagesOf(table)) {
      expect(() => {
        checkIpcHeader(message.dataHeader, message.dataBody)
      }).not.toThrow()
    }
  })

  it("accepts a schema nested a few levels deep", async () => {
    const [schemaMessage] = await messagesOf(nested(3))
    expect(() => {
      checkIpcHeader(schemaMessage.dataHeader, schemaMessage.dataBody)
    }).not.toThrow()
  })

  it("refuses a schema nested deeper than any real one", async () => {
    const [schemaMessage] = await messagesOf(nested(70))
    expect(() => {
      checkIpcHeader(schemaMessage.dataHeader, schemaMessage.dataBody)
    }).toThrow(FlightDecodeError)
  })

  it("refuses a record batch whose body is shorter than its header claims", async () => {
    const messages = await messagesOf(new Table({ id: vectorFromArray([1, 2, 3], new Int32()) }))
    const batch = messages[messages.length - 1]
    expect(() => {
      checkIpcHeader(batch.dataHeader, batch.dataBody.slice(0, 1))
    }).toThrow(FlightDecodeError)
  })

  it("lets the end-of-stream marker through as a header", () => {
    expect(() => {
      checkIpcHeader(Uint8Array.of(0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0), new Uint8Array())
    }).not.toThrow()
  })
})
