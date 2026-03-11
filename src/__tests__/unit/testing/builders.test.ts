/**
 * Tests for testing/builders.ts
 */
import { describe, expect, it } from "vitest"

import {
  batchesToFlightData,
  collectFlightData,
  createEmptyFlightData,
  tableToFlightData
} from "../../../testing/builders"
import { createIntegerTable, createTestBatch, createTestTable } from "../../../testing/helpers"
import { asyncIterable } from "../../../testing/streams"

describe("testing/builders", () => {
  describe("createEmptyFlightData", () => {
    it("returns a FlightData object", () => {
      const data = createEmptyFlightData()

      expect(data).toBeDefined()
    })

    it("has empty dataHeader", () => {
      const data = createEmptyFlightData()

      expect(data.dataHeader).toEqual(new Uint8Array(0))
    })

    it("has empty dataBody", () => {
      const data = createEmptyFlightData()

      expect(data.dataBody).toEqual(new Uint8Array(0))
    })

    it("has empty appMetadata", () => {
      const data = createEmptyFlightData()

      expect(data.appMetadata).toEqual(new Uint8Array(0))
    })
  })

  describe("collectFlightData", () => {
    it("collects all items from async iterable", async () => {
      const items = [createEmptyFlightData(), createEmptyFlightData()]
      const stream = asyncIterable(items)

      const result = await collectFlightData(stream)

      expect(result).toHaveLength(2)
    })

    it("returns empty array for empty stream", async () => {
      const stream = asyncIterable([])

      const result = await collectFlightData(stream)

      expect(result).toEqual([])
    })
  })

  describe("tableToFlightData", () => {
    it("converts table to FlightData array", async () => {
      const table = createIntegerTable(3)

      const result = await tableToFlightData(table)

      expect(result.length).toBeGreaterThan(0)
    })

    it("result contains IPC data", async () => {
      const table = createTestTable({ id: new Int32Array([1, 2, 3]) })

      const result = await tableToFlightData(table)

      // At least one FlightData with non-empty header
      expect(result.some((r) => r.dataHeader.length > 0)).toBe(true)
    })
  })

  describe("batchesToFlightData", () => {
    it("converts batches to FlightData", () => {
      const table = createIntegerTable(5)

      const result = batchesToFlightData(table.batches, table.schema)

      expect(result.length).toBeGreaterThan(0)
    })

    it("returns empty array for empty batch array", () => {
      const table = createIntegerTable(1)

      const result = batchesToFlightData([], table.schema)

      expect(result).toEqual([])
    })

    it("works with single batch", () => {
      const batch = createTestBatch({ id: new Int32Array([1, 2]) })
      const table = createTestTable({ id: new Int32Array([1, 2]) })

      const result = batchesToFlightData([batch], table.schema)

      expect(result.length).toBeGreaterThan(0)
      expect(result[0].dataHeader.length).toBeGreaterThan(0)
    })
  })
})
