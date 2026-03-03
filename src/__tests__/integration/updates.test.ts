/**
 * Integration tests for Flight SQL update operations.
 *
 * Requires a running Arrow Flight SQL server.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightSqlClient, type FlightSqlClient } from "../../client"
import { config } from "./config"

describe("Update Integration", () => {
  let client: FlightSqlClient

  beforeAll(async () => {
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
    client.close()
  })

  describe("executeUpdate", () => {
    it("executes INSERT statement", async () => {
      const result = await client.executeUpdate(
        `INSERT INTO ${config.tables.integers} (id, value) VALUES (999, 1)`
      )

      // recordCount should be -1 (unknown) or >= 0
      expect(result.recordCount).toBeGreaterThanOrEqual(-1n)
    })

    it("executes UPDATE statement", async () => {
      const result = await client.executeUpdate(
        `UPDATE ${config.tables.integers} SET value = 42 WHERE id = 999`
      )

      expect(result.recordCount).toBeGreaterThanOrEqual(-1n)
    })

    it("executes DELETE statement", async () => {
      const result = await client.executeUpdate(
        `DELETE FROM ${config.tables.integers} WHERE id = 999`
      )

      expect(result.recordCount).toBeGreaterThanOrEqual(-1n)
    })

    it("returns error for invalid SQL", async () => {
      await expect(client.executeUpdate("INVALID UPDATE")).rejects.toThrow()
    })
  })

  describe("read-only user cannot update", () => {
    let readerClient: FlightSqlClient

    beforeAll(async () => {
      readerClient = createFlightSqlClient({
        url: config.url,
        auth: {
          type: "basic",
          credentials: config.credentials.reader
        }
      })
      await readerClient.authenticate()
    })

    afterAll(() => {
      readerClient.close()
    })

    it("rejects INSERT with permission error", async () => {
      await expect(
        readerClient.executeUpdate(
          `INSERT INTO ${config.tables.integers} (id, value) VALUES (999, 1)`
        )
      ).rejects.toThrow()
    })
  })
})
