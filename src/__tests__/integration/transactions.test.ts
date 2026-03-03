/**
 * Integration tests for Flight SQL transactions.
 *
 * Requires a running Arrow Flight SQL server with transaction support.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest"

import { createFlightSqlClient, type FlightSqlClient, type Transaction } from "../../client"
import { config } from "./config"

describe("Transactions Integration", () => {
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

  describe("beginTransaction", () => {
    let transaction: Transaction | null = null

    afterEach(async () => {
      // Clean up any open transaction
      if (transaction !== null) {
        try {
          await client.rollback(transaction)
        } catch {
          // Ignore errors during cleanup
        }
        transaction = null
      }
    })

    it("begins a transaction", async () => {
      transaction = await client.beginTransaction()

      expect(transaction.id).toBeDefined()
      expect(transaction.id.length).toBeGreaterThan(0)
    })

    it("begins multiple transactions", async () => {
      const transaction1 = await client.beginTransaction()
      const transaction2 = await client.beginTransaction()

      expect(transaction1.id).toBeDefined()
      expect(transaction2.id).toBeDefined()
      // Transaction IDs should be unique
      expect(transaction1.id.toString()).not.toBe(transaction2.id.toString())

      // Clean up both
      await client.rollback(transaction1)
      await client.rollback(transaction2)
    })
  })

  describe("commit and rollback", () => {
    it("commits a transaction", async () => {
      const transaction = await client.beginTransaction()

      // Commit should succeed (even without executing anything)
      await client.commit(transaction)
    })

    it("rolls back a transaction", async () => {
      const transaction = await client.beginTransaction()

      // Rollback should succeed (even without executing anything)
      await client.rollback(transaction)
    })

    it("commits a transaction with pending updates", async () => {
      const transaction = await client.beginTransaction()

      // Execute updates within the transaction
      const result1 = await client.executeUpdate(
        `INSERT INTO ${config.tables.integers} (id, value) VALUES (997, 1)`,
        { transactionId: transaction.id }
      )
      expect(result1.recordCount).toBeGreaterThanOrEqual(-1n)

      const result2 = await client.executeUpdate(
        `UPDATE ${config.tables.integers} SET value = 42 WHERE id = 997`,
        { transactionId: transaction.id }
      )
      expect(result2.recordCount).toBeGreaterThanOrEqual(-1n)

      // Commit should succeed
      await client.commit(transaction)
    })

    it("rolls back a transaction with pending updates", async () => {
      const transaction = await client.beginTransaction()

      // Execute updates within the transaction
      const result = await client.executeUpdate(
        `DELETE FROM ${config.tables.integers} WHERE id = 1`,
        { transactionId: transaction.id }
      )
      expect(result.recordCount).toBeGreaterThanOrEqual(-1n)

      // Rollback should succeed and discard the pending delete
      await client.rollback(transaction)
    })
  })

  describe("query within transaction", () => {
    it("executes query within transaction", async () => {
      const transaction = await client.beginTransaction()

      try {
        // Query within transaction
        const table = await client.query(`SELECT * FROM ${config.tables.integers} LIMIT 100`, {
          transactionId: transaction.id
        })
        // LIMIT 100 should return at most 100 rows
        expect(table.numRows).toBeGreaterThan(0)
        expect(table.numRows).toBeLessThanOrEqual(100)
      } finally {
        await client.rollback(transaction)
      }
    })
  })
})
