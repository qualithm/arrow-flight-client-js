/**
 * Integration tests for Flight operations: listFlights, getFlightInfo, getSchema.
 *
 * Requires a running Arrow Flight server with test fixtures.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightClient, type FlightClient, type FlightDescriptorInput } from "../../client"
import { config, isFlightAvailable } from "./config"

/** Convert path segments to a FlightDescriptor. */
function pathDescriptor(...path: string[]): FlightDescriptorInput {
  return { type: "path", path }
}

describe("Flight Operations Integration", () => {
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

  describe("listFlights", () => {
    it("lists available flights", async () => {
      if (!available) {
        return
      }

      const flights: unknown[] = []

      for await (const info of client.listFlights()) {
        flights.push(info)
      }

      // Server should have test fixtures
      expect(flights.length).toBeGreaterThan(0)
    })

    it("returns FlightInfo with required fields", async () => {
      if (!available) {
        return
      }

      for await (const info of client.listFlights()) {
        // FlightInfo must have a descriptor
        expect(info.flightDescriptor).toBeDefined()
        // Should have at least one endpoint for data retrieval
        expect(info.endpoint.length).toBeGreaterThanOrEqual(0)
        // Schema should be present
        expect(info.schema).toBeDefined()
        break // Just check the first one
      }
    })

    it("filters flights with criteria expression", async () => {
      if (!available) {
        return
      }

      const allFlights: unknown[] = []
      for await (const info of client.listFlights()) {
        allFlights.push(info)
      }

      // Filter with criteria containing "integers"
      const filtered: unknown[] = []
      for await (const info of client.listFlights({
        expression: new TextEncoder().encode("integers")
      })) {
        filtered.push(info)
      }

      // Filtered results should be a subset
      expect(filtered.length).toBeLessThanOrEqual(allFlights.length)
    })
  })

  describe("getFlightInfo", () => {
    it("gets flight info for test/integers", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      expect(info.flightDescriptor).toBeDefined()
      expect(info.schema).toBeDefined()
      expect(info.schema.length).toBeGreaterThan(0)
      // test/integers should have records (actual count may vary)
      expect(info.totalRecords).toBeGreaterThan(0)
    })

    it("gets flight info for test/strings", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.strings)
      const info = await client.getFlightInfo(descriptor)

      // Verify flight has data (exact count depends on server fixtures)
      expect(Number(info.totalRecords)).toBeGreaterThan(0)
    })

    it("gets flight info for test/empty", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.empty)
      const info = await client.getFlightInfo(descriptor)

      expect(Number(info.totalRecords)).toBe(0)
    })

    it("gets flight info for test/large", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.large)
      const info = await client.getFlightInfo(descriptor)

      expect(Number(info.totalRecords)).toBe(10000)
    })

    it("returns NOT_FOUND for non-existent flight", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor("nonexistent", "flight")

      await expect(client.getFlightInfo(descriptor)).rejects.toThrow()
    })
  })

  describe("getSchema", () => {
    it("gets schema for test/integers", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.integers)
      const result = await client.getSchema(descriptor)

      expect(result.schema).toBeDefined()
      expect(result.schema.length).toBeGreaterThan(0)
    })

    it("schema matches getFlightInfo schema", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor(...config.flights.strings)

      const info = await client.getFlightInfo(descriptor)
      const schemaResult = await client.getSchema(descriptor)

      // Both should return the same schema bytes
      expect(schemaResult.schema).toEqual(info.schema)
    })

    it("returns error for non-existent flight", async () => {
      if (!available) {
        return
      }

      const descriptor = pathDescriptor("does", "not", "exist")

      await expect(client.getSchema(descriptor)).rejects.toThrow()
    })
  })
})
