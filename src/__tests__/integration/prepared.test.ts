/**
 * Integration tests for Flight SQL prepared statements.
 *
 * Requires a running Arrow Flight SQL server.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightSqlClient, type FlightSqlClient, type PreparedStatement } from "../../client"
import { config, isFlightAvailable } from "./config"

describe("Prepared Statements Integration", () => {
  let client: FlightSqlClient
  let available: boolean

  beforeAll(async () => {
    available = await isFlightAvailable()
    if (!available) {
      return
    }

    client = createFlightSqlClient({
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

  describe("prepare", () => {
    it("creates a prepared statement for SELECT query", async () => {
      if (!available) {
        return
      }

      const prepared = await client.prepare(`SELECT * FROM ${config.tables.integers}`)

      expect(prepared.handle).toBeDefined()
      expect(prepared.handle.length).toBeGreaterThan(0)
      // Dataset schema should be provided for queries
      expect(prepared.datasetSchema.length).toBeGreaterThan(0)

      // Clean up
      await client.closePreparedStatement(prepared)
    })

    it("creates a prepared statement for UPDATE query", async () => {
      if (!available) {
        return
      }

      const prepared = await client.prepare(
        `UPDATE ${config.tables.integers} SET value = 42 WHERE id = ?`
      )

      expect(prepared.handle).toBeDefined()
      expect(prepared.handle.length).toBeGreaterThan(0)

      // Clean up
      await client.closePreparedStatement(prepared)
    })

    it("creates a prepared statement with parameters", async () => {
      if (!available) {
        return
      }

      const prepared = await client.prepare(`SELECT * FROM ${config.tables.integers} WHERE id = ?`)

      expect(prepared.handle).toBeDefined()
      // Parameter schema should describe the parameter
      // (may be empty if server doesn't track parameters)
      expect(prepared.parameterSchema).toBeDefined()

      // Clean up
      await client.closePreparedStatement(prepared)
    })

    it("rejects invalid SQL", async () => {
      if (!available) {
        return
      }

      await expect(client.prepare("INVALID SQL")).rejects.toThrow()
    })
  })

  describe("executePrepared", () => {
    let prepared: PreparedStatement

    beforeAll(async () => {
      if (!available) {
        return
      }

      prepared = await client.prepare(`SELECT * FROM ${config.tables.integers}`)
    })

    afterAll(async () => {
      if (!available) {
        return
      }

      await client.closePreparedStatement(prepared)
    })

    it("executes a prepared query", async () => {
      if (!available) {
        return
      }

      const table = await client.executePrepared(prepared)

      expect(table).toBeDefined()
      // Verify query returned data (exact count depends on server fixtures)
      expect(table.numRows).toBeGreaterThan(0)
    })

    it("can execute multiple times", async () => {
      if (!available) {
        return
      }

      // Execute first time
      const table1 = await client.executePrepared(prepared)

      // Execute second time
      const table2 = await client.executePrepared(prepared)

      // Both executions should return the same results
      expect(table1.numRows).toBeGreaterThan(0)
      expect(table2.numRows).toBe(table1.numRows)
    })
  })

  describe("executePreparedUpdate", () => {
    it("executes a prepared update", async () => {
      if (!available) {
        return
      }

      const prepared = await client.prepare(
        `INSERT INTO ${config.tables.integers} (id, value) VALUES (998, 1)`
      )

      const result = await client.executePreparedUpdate(prepared)
      expect(result.recordCount).toBeGreaterThanOrEqual(-1n)

      // Clean up
      await client.closePreparedStatement(prepared)
    })
  })

  describe("closePreparedStatement", () => {
    it("closes a prepared statement", async () => {
      if (!available) {
        return
      }

      const prepared = await client.prepare(`SELECT * FROM ${config.tables.integers}`)

      // Should not throw
      await client.closePreparedStatement(prepared)

      // Attempting to use closed handle should fail
      await expect(client.executePrepared(prepared)).rejects.toThrow()
    })
  })
})
