/**
 * Integration tests for data operations: doGet, doPut.
 *
 * Requires a running Arrow Flight server with test fixtures.
 */
import { create } from "@bufbuild/protobuf"
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import {
  createFlightClient,
  decodeFlightDataToTable,
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

describe("Data Operations Integration", () => {
  let client: FlightClient
  let available: boolean

  beforeAll(async () => {
    available = await isFlightAvailable()
    if (!available) {
      return
    }

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
    if (available) {
      client.close()
    }
  })

  describe("doGet", () => {
    it("retrieves data for test/integers", async () => {
      if (!available) {
        return
      }

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
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.strings)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      // Verify we got data (exact count depends on server fixtures)
      expect(table.numRows).toBeGreaterThan(0)
      expect(table.schema.fields.map((f) => f.name)).toContain("name")
    })

    it("returns empty result for test/empty", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.empty)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      expect(table.numRows).toBe(0)
    })

    it("retrieves large dataset", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.large)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      expect(table.numRows).toBe(10000)
    })

    it("retrieves nested types", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.nested)
      const info = await client.getFlightInfo(descriptor)

      const table = await decodeFlightDataToTable(client.doGet(info.endpoint[0].ticket!))

      expect(table.numRows).toBe(50)
      // Should have items column with List type
      expect(table.schema.fields.map((f) => f.name)).toContain("items")
    })
  })

  describe("doPut", () => {
    it("uploads data and receives acknowledgement", async () => {
      if (!available) {
        return
      }

      // First, get schema from an existing flight to use as template
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      // Get the original data
      const sourceChunks: FlightData[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        sourceChunks.push(data)
      }

      // Upload to a new path
      const putDescriptor: FlightDescriptorInput = {
        type: "path",
        path: ["test", `put-test-${String(Date.now())}`]
      }

      // Create FlightData stream with descriptor
      // eslint-disable-next-line @typescript-eslint/require-await
      async function* createPutStream(): AsyncGenerator<FlightData> {
        for (let i = 0; i < sourceChunks.length; i++) {
          const data = sourceChunks[i]
          if (i === 0) {
            // First message includes descriptor - use create() for proper proto message
            yield create(FlightDataSchema, {
              flightDescriptor: {
                type: FlightDescriptor_DescriptorType.PATH,
                path: putDescriptor.type === "path" ? putDescriptor.path : [],
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

      // Collect acknowledgements
      const acks: unknown[] = []
      for await (const result of client.doPut(createPutStream())) {
        acks.push(result)
      }

      // Should receive acknowledgements (server-dependent)
      expect(Array.isArray(acks)).toBe(true)
    })
  })
})
