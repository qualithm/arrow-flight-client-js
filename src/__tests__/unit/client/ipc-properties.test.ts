import { tableFromArrays } from "apache-arrow"
import fc from "fast-check"
import { describe, expect, it } from "vitest"

import { FlightDecodeError } from "../../../client/errors.js"
import { decodeFlightDataToTable, encodeTableToFlightData } from "../../../client/ipc.js"
import type { FlightData } from "../../../gen/arrow/flight/Flight_pb.js"

/**
 * Replays a failure: fast-check prints `seed` and `path` with the counterexample, and
 * `FC_SEED=<seed> FC_PATH=<path> bun run test` runs exactly that case again.
 */
function replay(): fc.Parameters<unknown> {
  const seed = process.env.FC_SEED
  const path = process.env.FC_PATH
  return seed === undefined ? {} : { seed: Number(seed), path }
}

function flightData(dataHeader: Uint8Array, dataBody: Uint8Array): FlightData {
  return {
    $typeName: "arrow.flight.protocol.FlightData",
    flightDescriptor: undefined,
    dataHeader,
    appMetadata: new Uint8Array(),
    dataBody
  }
}

async function* streamOf(messages: readonly FlightData[]): AsyncGenerator<FlightData> {
  for (const message of messages) {
    await Promise.resolve()
    yield message
  }
}

async function collect(table: ReturnType<typeof tableFromArrays>): Promise<FlightData[]> {
  const messages: FlightData[] = []
  for await (const message of encodeTableToFlightData(table)) {
    messages.push(message)
  }
  return messages
}

/** Decodes, or rejects with FlightDecodeError (or the empty-stream error); any other throw fails. */
async function decodeOrTypedError(messages: readonly FlightData[]): Promise<void> {
  try {
    await decodeFlightDataToTable(streamOf(messages))
  } catch (error) {
    const typed =
      error instanceof FlightDecodeError ||
      (error instanceof Error && error.message === "no record batches received from stream")
    expect(typed, String(error)).toBe(true)
  }
}

const arbRows = fc.array(
  fc.record({ id: fc.integer({ min: -(2 ** 31), max: 2 ** 31 - 1 }), name: fc.string() }),
  { minLength: 1, maxLength: 50 }
)

describe("flight data decoder properties", () => {
  it("rejects a record batch whose body is cut short with FlightDecodeError", async () => {
    const messages = await collect(tableFromArrays({ id: Int32Array.from([1, 2, 3, 4]) }))
    const batch = messages[messages.length - 1]
    const cut = messages.map((message) =>
      message === batch ? flightData(message.dataHeader, message.dataBody.slice(0, 4)) : message
    )
    const error = await decodeFlightDataToTable(streamOf(cut)).catch((thrown: unknown) => thrown)
    expect(error).toBeInstanceOf(FlightDecodeError)
  })

  it("decodes what it encodes", async () => {
    await fc.assert(
      fc.asyncProperty(arbRows, async (rows) => {
        const table = tableFromArrays({
          id: Int32Array.from(rows.map((row) => row.id)),
          name: rows.map((row) => row.name)
        })
        const decoded = await decodeFlightDataToTable(streamOf(await collect(table)))
        expect(decoded.toArray().map((row) => ({ ...row }))).toEqual(rows)
      }),
      replay()
    )
  })

  it("answers arbitrary bytes with batches or a typed decode error, never another throw", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.tuple(fc.uint8Array({ maxLength: 256 }), fc.uint8Array({ maxLength: 256 })), {
          minLength: 1,
          maxLength: 4
        }),
        async (parts) => {
          await decodeOrTypedError(parts.map(([header, body]) => flightData(header, body)))
        }
      ),
      replay()
    )
  })

  it("answers a corrupted valid stream with batches or a typed decode error", async () => {
    await fc.assert(
      fc.asyncProperty(
        arbRows,
        fc.nat(),
        fc.nat(),
        fc.integer({ min: 0, max: 255 }),
        async (rows, which, at, value) => {
          const messages = await collect(
            tableFromArrays({
              id: Int32Array.from(rows.map((row) => row.id)),
              name: rows.map((row) => row.name)
            })
          )
          const target = messages[which % messages.length]
          const bytes = Uint8Array.from(target.dataHeader)
          if (bytes.length > 0) {
            bytes[at % bytes.length] = value
          }
          const corrupted = messages.map((message) =>
            message === target ? flightData(bytes, message.dataBody) : message
          )
          await decodeOrTypedError(corrupted)
        }
      ),
      replay()
    )
  })

  it("refuses the corrupted header that once exhausted memory", async () => {
    const messages = await collect(tableFromArrays({ id: Int32Array.from([0, 0]), name: ["", ""] }))
    const batch = messages[2]
    const header = Uint8Array.from(batch.dataHeader)
    header[79] = 6
    const error = await decodeFlightDataToTable(
      streamOf([messages[0], messages[1], flightData(header, batch.dataBody)])
    ).catch((thrown: unknown) => thrown)
    expect(error).toBeInstanceOf(FlightDecodeError)
  })
})
