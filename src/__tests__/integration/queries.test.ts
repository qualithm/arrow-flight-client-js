/**
 * Integration tests for Flight SQL query execution.
 *
 * Requires a running Arrow Flight SQL server with test fixtures.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightSqlClient, type FlightSqlClient } from "../../client"
import { config } from "./config"

describe("Query Integration", () => {
  let client: FlightSqlClient

  beforeAll(async () => {
    // Use bearer token auth if available, otherwise basic auth
    const auth =
      config.bearerToken !== undefined
        ? { type: "bearer" as const, token: config.bearerToken }
        : { type: "basic" as const, credentials: config.credentials.admin }

    client = createFlightSqlClient({
      url: config.url,
      auth
    })

    // Only authenticate if using basic auth (bearer token is already set)
    if (config.bearerToken === undefined) {
      await client.authenticate()
    }
  })

  afterAll(() => {
    client.close()
  })

  describe("query", () => {
    it("executes SELECT * query on test.integers", async () => {
      const table = await client.query(`SELECT * FROM ${config.tables.integers}`)

      expect(table).toBeDefined()
      expect(table.numRows).toBeGreaterThan(0)
    })

    it("returns correct row count", async () => {
      const table = await client.query(`SELECT * FROM ${config.tables.integers}`)

      // Verify we got data (exact count depends on server fixtures)
      expect(table.numRows).toBeGreaterThan(0)
    })

    it("handles empty result set", async () => {
      const table = await client.query(`SELECT * FROM ${config.tables.empty}`)

      expect(table.numRows).toBe(0)
    })

    it("returns error for invalid SQL", async () => {
      await expect(client.query("INVALID SQL SYNTAX")).rejects.toThrow()
    })

    it("returns error for non-existent table", async () => {
      await expect(client.query("SELECT * FROM nonexistent_table")).rejects.toThrow()
    })
  })

  describe("queryBatches", () => {
    it("returns Arrow RecordBatches", async () => {
      let totalRows = 0

      for await (const batch of client.queryBatches(
        `SELECT * FROM ${config.tables.integers} LIMIT 100`
      )) {
        totalRows += batch.numRows
      }

      // LIMIT 100 should return at most 100 rows
      expect(totalRows).toBeGreaterThan(0)
      expect(totalRows).toBeLessThanOrEqual(100)
    })

    it("has correct schema fields for integers table", async () => {
      for await (const batch of client.queryBatches(`SELECT * FROM ${config.tables.integers}`)) {
        const fieldNames = batch.schema.fields.map((f) => f.name)
        expect(fieldNames).toContain("id")
        expect(fieldNames).toContain("value")
        break // Just check the first batch
      }
    })

    it("has correct schema fields for strings table", async () => {
      for await (const batch of client.queryBatches(`SELECT * FROM ${config.tables.strings}`)) {
        const fieldNames = batch.schema.fields.map((f) => f.name)
        expect(fieldNames).toContain("id")
        expect(fieldNames).toContain("name")
        break
      }
    })

    it("handles large datasets", async () => {
      let totalRows = 0

      for await (const batch of client.queryBatches(`SELECT * FROM ${config.tables.large}`)) {
        totalRows += batch.numRows
      }

      expect(totalRows).toBe(10000)
    })

    it("handles nested types", async () => {
      for await (const batch of client.queryBatches(`SELECT * FROM ${config.tables.nested}`)) {
        const fieldNames = batch.schema.fields.map((f) => f.name)
        expect(fieldNames).toContain("items")
        break
      }
    })
  })

  describe("getQueryInfo", () => {
    it("returns FlightInfo for query", async () => {
      const info = await client.getQueryInfo(`SELECT * FROM ${config.tables.integers}`)

      expect(info).toBeDefined()
      expect(info.endpoint.length).toBeGreaterThan(0)
    })

    it("returns schema information", async () => {
      const info = await client.getQueryInfo(`SELECT * FROM ${config.tables.integers}`)

      expect(info.schema).toBeDefined()
      expect(info.schema.length).toBeGreaterThan(0)
    })
  })
})
