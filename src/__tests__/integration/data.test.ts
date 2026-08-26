/**
 * Integration tests for data operations: doGet, doPut.
 *
 * Requires a running Arrow Flight server with test fixtures.
 */
import { create } from "@bufbuild/protobuf"
import { tableFromArrays } from "apache-arrow"
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import {
  createFlightClient,
  decodeFlightDataToTable,
  encodeTableToFlightData,
  type FlightClient,
  type FlightDescriptorInput
} from "../../client"
import {
  type FlightData,
  FlightDataSchema,
  FlightDescriptor_DescriptorType
} from "../../gen/arrow/flight/Flight_pb"
import { config, isFlightAvailable } from "./config"

/** Convert path segments to a FlightDescriptor. */
function pathDescriptor(...path: string[]): FlightDescriptorInput {
  return { type: "path", path }
}

/** Attach a path FlightDescriptor to the first message of an encoded stream. */
async function* withDescriptor(
  stream: AsyncGenerator<FlightData>,
  path: string[]
): AsyncGenerator<FlightData> {
  let first = true
  for await (const data of stream) {
    if (first) {
      first = false
      yield create(FlightDataSchema, {
        flightDescriptor: {
          type: FlightDescriptor_DescriptorType.PATH,
          path,
          cmd: new Uint8Array()
        },
        dataHeader: data.dataHeader,
        dataBody: data.dataBody,
        appMetadata: data.appMetadata
      })
    } else {
      yield data
    }
  }
}

// Probed once at module load so the skip is explicit in the run summary
// rather than a silent pass with zero assertions.
const serverAvailable = await isFlightAvailable()

describe.skipIf(!serverAvailable)("Data Operations Integration", () => {
  let client: FlightClient

  beforeAll(async () => {
    client = createFlightClient({
      url: config.url,
      auth: {
        type: "basic",
        credentials: config.credentials.admin
      }
    })
    await client.authenticate()
  })

  afterAll(() => {
    client.close()
  })

  describe("doGet", () => {
    it("retrieves data for test/integers", async () => {
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      // Should have at least one endpoint
      expect(info.endpoint.length).toBeGreaterThan(0)

      const endpoint = info.endpoint[0]
      expect(endpoint.ticket).toBeDefined()

      // Collect FlightData and convert to Table
      const table = await decodeFlightDataToTable(client.doGet(endpoint.ticket!))

      // Should have received data
      expect(table.numRows).toBeGreaterThan(0)
      // Row count should match what FlightInfo reported
      expect(table.numRows).toBe(Number(info.totalRecords))
      // Schema should have id and value columns
      expect(table.schema.fields.map((f) => f.name)).toContain("id")
      expect(table.schema.fields.map((f) => f.name)).toContain("value")
    })

    it("retrieves data for test/strings", async () => {
      const descriptor = pathDescriptor(...config.flights.strings)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      // Verify we got data (exact count depends on server fixtures)
      expect(table.numRows).toBeGreaterThan(0)
      expect(table.schema.fields.map((f) => f.name)).toContain("name")
    })

    it("returns empty result for test/empty", async () => {
      const descriptor = pathDescriptor(...config.flights.empty)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      expect(table.numRows).toBe(0)
    })

    it("retrieves large dataset", async () => {
      const descriptor = pathDescriptor(...config.flights.large)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      expect(table.numRows).toBe(10000)
    })

    it("retrieves nested types", async () => {
      const descriptor = pathDescriptor(...config.flights.nested)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      expect(table.numRows).toBe(50)
      // Should have items column with List type
      expect(table.schema.fields.map((f) => f.name)).toContain("items")
    })
  })

  describe("doPut", () => {
    // Exercise the public encode path end to end: build a Table, encode with
    // encodeTableToFlightData (where the #212 wire-format bugs lived), upload,
    // then read back and assert the exact rows. A string column covers the
    // dictionary-encoded batch path.
    it("round-trips an encoded Table through doPut and doGet", async () => {
      const ids = [1, 2, 3, 4, 5]
      const names = ["alpha", "beta", "gamma", "delta", "epsilon"]
      const table = tableFromArrays({ id: ids, name: names })
      expect(table.numRows).toBe(5)

      const path = ["test", `put-test-${String(Date.now())}`]

      const acks: unknown[] = []
      for await (const result of client.doPut(
        withDescriptor(encodeTableToFlightData(table), path)
      )) {
        acks.push(result)
      }
      expect(acks.length).toBeGreaterThan(0)

      const readBack = await decodeFlightDataToTable(
        client.doGet((await client.getFlightInfo(pathDescriptor(...path))).endpoint[0].ticket!)
      )

      expect(readBack.numRows).toBe(5)
      expect(readBack.schema.fields.map((f) => f.name)).toEqual(["id", "name"])

      const readIds = readBack.getChild("id")?.toArray()
      const readNames = readBack.getChild("name")?.toArray()
      expect([...readIds]).toEqual(ids)
      expect([...readNames].map(String)).toEqual(names)
    })
  })
})
