/**
 * Tests for testing/helpers.ts
 */
import { describe, expect, it } from "vitest"

import {
  createFloatTable,
  createIntegerTable,
  createStringTable,
  createTestBatch,
  createTestTable
} from "../../../testing/helpers"

describe("testing/helpers", () => {
  describe("createTestTable", () => {
    it("creates a table from Int32Array columns", () => {
      const table = createTestTable({
        id: new Int32Array([1, 2, 3])
      })

      expect(table.numRows).toBe(3)
      expect(table.numCols).toBe(1)
      expect(table.schema.fields[0].name).toBe("id")
    })

    it("creates a table from Float64Array columns", () => {
      const table = createTestTable({
        value: new Float64Array([1.1, 2.2, 3.3])
      })

      expect(table.numRows).toBe(3)
      expect(table.schema.fields[0].name).toBe("value")
    })

    it("creates a table from string arrays", () => {
      const table = createTestTable({
        name: ["alice", "bob", "charlie"]
      })

      expect(table.numRows).toBe(3)
      expect(table.schema.fields[0].name).toBe("name")
    })

    it("creates a table with multiple columns", () => {
      const table = createTestTable({
        id: new Int32Array([1, 2]),
        value: new Float64Array([1.1, 2.2]),
        name: ["a", "b"]
      })

      expect(table.numRows).toBe(2)
      expect(table.numCols).toBe(3)
    })
  })

  describe("createTestBatch", () => {
    it("returns a single RecordBatch", () => {
      const batch = createTestBatch({
        id: new Int32Array([1, 2, 3])
      })

      expect(batch.numRows).toBe(3)
      expect(batch.numCols).toBe(1)
    })

    it("batch has correct schema", () => {
      const batch = createTestBatch({
        id: new Int32Array([1]),
        value: new Float64Array([1.1])
      })

      expect(batch.schema.fields.map((f) => f.name)).toEqual(["id", "value"])
    })
  })

  describe("createIntegerTable", () => {
    it("creates table with default count (10)", () => {
      const table = createIntegerTable()

      expect(table.numRows).toBe(10)
    })

    it("creates table with specified count", () => {
      const table = createIntegerTable(5)

      expect(table.numRows).toBe(5)
    })

    it("has id and value columns", () => {
      const table = createIntegerTable(3)

      expect(table.schema.fields.map((f) => f.name)).toEqual(["id", "value"])
    })

    it("has sequential ids starting at 1", () => {
      const table = createIntegerTable(3)
      const batch = table.batches[0]
      const idCol = batch.getChildAt(0)!

      expect(idCol.get(0)).toBe(1)
      expect(idCol.get(1)).toBe(2)
      expect(idCol.get(2)).toBe(3)
    })

    it("has values that are id * 10", () => {
      const table = createIntegerTable(3)
      const batch = table.batches[0]
      const valueCol = batch.getChildAt(1)!

      expect(valueCol.get(0)).toBe(10)
      expect(valueCol.get(1)).toBe(20)
      expect(valueCol.get(2)).toBe(30)
    })
  })

  describe("createFloatTable", () => {
    it("creates table with default count (10)", () => {
      const table = createFloatTable()

      expect(table.numRows).toBe(10)
    })

    it("creates table with specified count", () => {
      const table = createFloatTable(7)

      expect(table.numRows).toBe(7)
    })

    it("has id and value columns", () => {
      const table = createFloatTable(2)

      expect(table.schema.fields.map((f) => f.name)).toEqual(["id", "value"])
    })

    it("has float values", () => {
      const table = createFloatTable(2)
      const batch = table.batches[0]
      const valueCol = batch.getChildAt(1)!

      expect(valueCol.get(0)).toBeCloseTo(1.1, 5)
      expect(valueCol.get(1)).toBeCloseTo(2.2, 5)
    })
  })

  describe("createStringTable", () => {
    it("creates table with default count (10)", () => {
      const table = createStringTable()

      expect(table.numRows).toBe(10)
    })

    it("creates table with specified count", () => {
      const table = createStringTable(4)

      expect(table.numRows).toBe(4)
    })

    it("has id and name columns", () => {
      const table = createStringTable(2)

      expect(table.schema.fields.map((f) => f.name)).toEqual(["id", "name"])
    })

    it("has name values in expected format", () => {
      const table = createStringTable(3)
      const batch = table.batches[0]
      const nameCol = batch.getChildAt(1)!

      expect(nameCol.get(0)).toBe("item-1")
      expect(nameCol.get(1)).toBe("item-2")
      expect(nameCol.get(2)).toBe("item-3")
    })
  })
})
