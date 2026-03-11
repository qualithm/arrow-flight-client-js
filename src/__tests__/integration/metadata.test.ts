/**
 * Integration tests for Flight SQL database metadata queries.
 *
 * Requires a running Arrow Flight SQL server.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightSqlClient, type FlightSqlClient } from "../../client"
import { config, isFlightAvailable } from "./config"

describe("Metadata Integration", () => {
  let client: FlightSqlClient
  let available: boolean

  beforeAll(async () => {
    available = await isFlightAvailable()
    if (!available) {
      return
    }

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
    if (available) {
      client.close()
    }
  })

  describe("getCatalogs", () => {
    it("returns a table with catalog_name column", async () => {
      if (!available) {
        return
      }

      const catalogs = await client.getCatalogs()

      expect(catalogs).toBeDefined()
      // Should have catalog_name column
      const fieldNames = catalogs.schema.fields.map((f) => f.name)
      expect(fieldNames).toContain("catalog_name")
    })

    it("returns at least one catalog", async () => {
      if (!available) {
        return
      }

      const catalogs = await client.getCatalogs()

      expect(catalogs.numRows).toBeGreaterThanOrEqual(1)
    })
  })

  describe("getDbSchemas", () => {
    it("returns a table with schema columns", async () => {
      if (!available) {
        return
      }

      const schemas = await client.getDbSchemas()

      expect(schemas).toBeDefined()
      const fieldNames = schemas.schema.fields.map((f) => f.name)
      expect(fieldNames).toContain("db_schema_name")
    })

    it("filters by catalog", async () => {
      if (!available) {
        return
      }

      const schemas = await client.getDbSchemas({ catalog: config.catalog })

      expect(schemas).toBeDefined()
    })
  })

  describe("getTables", () => {
    it("returns a table with table columns", async () => {
      if (!available) {
        return
      }

      const tables = await client.getTables()

      expect(tables).toBeDefined()
      const fieldNames = tables.schema.fields.map((f) => f.name)
      expect(fieldNames).toContain("table_name")
      expect(fieldNames).toContain("table_type")
    })

    it("returns test tables", async () => {
      if (!available) {
        return
      }

      const tables = await client.getTables()

      expect(tables.numRows).toBeGreaterThan(0)
    })

    it("filters by table type", async () => {
      if (!available) {
        return
      }

      const tables = await client.getTables({ tableTypes: ["TABLE"] })

      expect(tables).toBeDefined()
    })

    it("filters by table name pattern", async () => {
      if (!available) {
        return
      }

      const tables = await client.getTables({ tableNameFilterPattern: "integers" })

      expect(tables).toBeDefined()
      // Should have at least the integers table
      expect(tables.numRows).toBeGreaterThanOrEqual(1)
    })
  })

  describe("getTableTypes", () => {
    it("returns a table with table_type column", async () => {
      if (!available) {
        return
      }

      const tableTypes = await client.getTableTypes()

      expect(tableTypes).toBeDefined()
      const fieldNames = tableTypes.schema.fields.map((f) => f.name)
      expect(fieldNames).toContain("table_type")
    })

    it("returns at least TABLE type", async () => {
      if (!available) {
        return
      }

      const tableTypes = await client.getTableTypes()

      expect(tableTypes.numRows).toBeGreaterThanOrEqual(1)
    })
  })

  describe("getPrimaryKeys", () => {
    it("returns primary key information for a table", async () => {
      if (!available) {
        return
      }

      const keys = await client.getPrimaryKeys("integers", {
        catalog: config.catalog,
        dbSchema: "test"
      })

      expect(keys).toBeDefined()
      // Should have column_name field
      const fieldNames = keys.schema.fields.map((f) => f.name)
      expect(fieldNames).toContain("column_name")
    })
  })
})
